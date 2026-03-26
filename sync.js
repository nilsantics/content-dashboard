require('dotenv').config();
const cron = require('node-cron');
const { Pool } = require('pg');

function parseDurationSecs(str) {
  if (!str) return 0;
  const m = str.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return 0;
  return (parseInt(m[1]||0)*3600) + (parseInt(m[2]||0)*60) + (parseInt(m[3]||0));
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

pool.query(`
  CREATE TABLE IF NOT EXISTS subscriber_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    platform    TEXT NOT NULL,
    count       BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
`).catch(err => console.warn('subscriber_snapshots init:', err.message));

pool.query(`
  CREATE TABLE IF NOT EXISTS daily_analytics (
    id            BIGSERIAL PRIMARY KEY,
    platform      TEXT NOT NULL,
    date          DATE NOT NULL,
    views         BIGINT DEFAULT 0,
    likes         BIGINT DEFAULT 0,
    comments      BIGINT DEFAULT 0,
    watch_minutes BIGINT DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(platform, date)
  )
`).catch(err => console.warn('daily_analytics init:', err.message));

// ─── Instagram ───────────────────────────────────────────────────────────────

async function fetchInstagramPosts() {
  const token = process.env.INSTAGRAM_ACCESS_TOKEN;
  const fields = 'id,caption,media_type,media_url,thumbnail_url,timestamp,like_count,comments_count';
  const url = `https://graph.instagram.com/v21.0/me/media?fields=${fields}&limit=25&access_token=${token}`;

  const res = await fetch(url);
  const data = await res.json();
  if (data.error) throw new Error(`Instagram media fetch failed: ${data.error.message}`);
  return data.data || [];
}

async function fetchInstagramInsights(mediaId, mediaType) {
  const token = process.env.INSTAGRAM_ACCESS_TOKEN;

  // Reels use 'views'; Images/Carousels use 'impressions'
  const isVideo = mediaType === 'VIDEO' || mediaType === 'REELS';
  const viewMetric = isVideo ? 'views' : 'impressions';
  const metrics = `${viewMetric},reach,saved,shares`;

  const url = `https://graph.instagram.com/v21.0/${mediaId}/insights?metric=${metrics}&period=lifetime&access_token=${token}`;

  const res = await fetch(url);
  const data = await res.json();
  if (data.error) return {};

  const result = {};
  for (const item of data.data || []) {
    result[item.name] = item.total_value?.value ?? item.values?.[0]?.value ?? 0;
  }
  return result;
}

async function syncInstagram() {
  const posts = await fetchInstagramPosts();
  const records = [];

  for (const post of posts) {
    const insights = await fetchInstagramInsights(post.id, post.media_type);

    const likes    = post.like_count     || 0;
    const comments = post.comments_count || 0;
    const shares   = insights.shares     || 0;
    const saves    = insights.saved      || 0;
    const reach    = insights.reach      || 0;
    // 'views' = Reels view count, 'impressions' = image/carousel view count
    const views    = insights.views ?? insights.impressions ?? 0;

    const engagement_rate = reach > 0
      ? Math.round(((likes + comments + shares + saves) / reach) * 10000) / 100
      : 0;

    records.push({
      platform:      'instagram',
      post_id:       post.id,
      title:         post.caption ? post.caption.substring(0, 500) : null,
      thumbnail_url: post.thumbnail_url || post.media_url || null,
      published_at:  post.timestamp,
      views,
      likes,
      comments,
      shares,
      saves,
      reach,
      engagement_rate,
    });
  }

  return records;
}

// ─── YouTube ─────────────────────────────────────────────────────────────────

async function getGoogleAccessToken() {
  if (!process.env.GOOGLE_REFRESH_TOKEN) return null;
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id:     process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
      grant_type:    'refresh_token',
    }),
  });
  const data = await res.json();
  if (data.error) { console.warn('  Google token refresh failed:', data.error_description); return null; }
  return data.access_token;
}

async function fetchYouTubeAnalytics(videoIds, accessToken) {
  if (!accessToken || !videoIds.length) return {};
  const today     = new Date().toISOString().split('T')[0];
  const url = new URL('https://youtubeanalytics.googleapis.com/v2/reports');
  url.searchParams.set('ids',        'channel==MINE');
  url.searchParams.set('dimensions', 'video');
  url.searchParams.set('metrics',    'views,estimatedMinutesWatched,averageViewDuration');
  url.searchParams.set('startDate',  '2020-01-01');
  url.searchParams.set('endDate',    today);
  url.searchParams.set('filters',    `video==${videoIds.join(',')}`);

  const res  = await fetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
  const data = await res.json();
  if (data.error) { console.warn('  YouTube Analytics API error:', data.error.message); return {}; }

  // Build a map: videoId → analytics row
  const map = {};
  const cols = (data.columnHeaders || []).map(h => h.name);
  const vi   = cols.indexOf('video');
  const ew   = cols.indexOf('estimatedMinutesWatched');
  const avd  = cols.indexOf('averageViewDuration');

  for (const row of data.rows || []) {
    map[row[vi]] = {
      watch_time_minutes: ew  >= 0 ? row[ew]  : null,
      avg_view_duration:  avd >= 0 ? Math.round(row[avd]) : null,
    };
  }
  return map;
}

async function syncYoutube() {
  const apiKey    = process.env.YOUTUBE_API_KEY;
  const channelId = process.env.YOUTUBE_CHANNEL_ID;
  if (!channelId) throw new Error('YOUTUBE_CHANNEL_ID is not set in .env');

  // Fetch 25 most recent video IDs
  const searchUrl  = `https://www.googleapis.com/youtube/v3/search?part=id&channelId=${channelId}&type=video&maxResults=25&order=date&key=${apiKey}`;
  const searchData = await fetch(searchUrl).then(r => r.json());
  if (searchData.error) throw new Error(`YouTube search failed: ${searchData.error.message}`);

  const videoIds = searchData.items.map(item => item.id.videoId);
  if (!videoIds.length) return [];

  // Fetch snippet + statistics + contentDetails (for duration/Shorts detection)
  const statsData = await fetch(
    `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id=${videoIds.join(',')}&key=${apiKey}`
  ).then(r => r.json());
  if (statsData.error) throw new Error(`YouTube videos fetch failed: ${statsData.error.message}`);

  // Fetch YouTube Analytics (CTR, AVD, watch time) if OAuth is set up
  const accessToken = await getGoogleAccessToken();
  const analytics   = await fetchYouTubeAnalytics(videoIds, accessToken);
  if (accessToken) console.log(`  ↳ YouTube Analytics: ${Object.keys(analytics).length} videos with CTR/AVD data`);

  return statsData.items.map(video => {
    const s     = video.statistics;
    const views = parseInt(s.viewCount    || 0, 10);
    const likes = parseInt(s.likeCount    || 0, 10);
    const comms = parseInt(s.commentCount || 0, 10);
    const a     = analytics[video.id] || {};
    const thumbs = video.snippet.thumbnails;
    const durationSecs = parseDurationSecs(video.contentDetails?.duration);

    return {
      platform:           'youtube',
      post_id:            video.id,
      title:              video.snippet.title,
      thumbnail_url:      thumbs?.maxres?.url || thumbs?.standard?.url || thumbs?.high?.url || null,
      published_at:       video.snippet.publishedAt,
      views,
      likes,
      comments:           comms,
      shares:             null,
      saves:              null,
      reach:              null,
      engagement_rate:    views > 0 ? Math.round(((likes + comms) / views) * 10000) / 100 : 0,
      ctr:                a.ctr               ?? null,
      avg_view_duration:  a.avg_view_duration ?? null,
      watch_time_minutes: a.watch_time_minutes ?? null,
      yt_impressions:     a.yt_impressions    ?? null,
      duration_secs:      durationSecs || null,
    };
  });
}

// ─── Supabase upsert ─────────────────────────────────────────────────────────

async function upsertRecords(records) {
  for (const r of records) {
    await pool.query(
      `INSERT INTO content_analytics
         (platform, post_id, title, thumbnail_url, published_at,
          views, likes, comments, shares, saves, reach, engagement_rate,
          ctr, avg_view_duration, watch_time_minutes, yt_impressions, duration_secs, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW())
       ON CONFLICT (post_id) DO UPDATE SET
         views               = EXCLUDED.views,
         likes               = EXCLUDED.likes,
         comments            = EXCLUDED.comments,
         shares              = EXCLUDED.shares,
         saves               = EXCLUDED.saves,
         reach               = EXCLUDED.reach,
         engagement_rate     = EXCLUDED.engagement_rate,
         ctr                 = COALESCE(EXCLUDED.ctr,               content_analytics.ctr),
         avg_view_duration   = COALESCE(EXCLUDED.avg_view_duration,  content_analytics.avg_view_duration),
         watch_time_minutes  = COALESCE(EXCLUDED.watch_time_minutes, content_analytics.watch_time_minutes),
         yt_impressions      = COALESCE(EXCLUDED.yt_impressions,     content_analytics.yt_impressions),
         duration_secs       = COALESCE(EXCLUDED.duration_secs,      content_analytics.duration_secs),
         updated_at          = NOW()`,
      [r.platform, r.post_id, r.title, r.thumbnail_url, r.published_at,
       r.views, r.likes, r.comments, r.shares, r.saves, r.reach, r.engagement_rate,
       r.ctr ?? null, r.avg_view_duration ?? null, r.watch_time_minutes ?? null,
       r.yt_impressions ?? null, r.duration_secs ?? null]
    );
  }
}

// ─── Daily YouTube Analytics ─────────────────────────────────────────────────

async function syncDailyYouTubeAnalytics() {
  const accessToken = await getGoogleAccessToken();
  if (!accessToken) return;

  const endDate   = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - 90 * 864e5).toISOString().split('T')[0];

  const url = new URL('https://youtubeanalytics.googleapis.com/v2/reports');
  url.searchParams.set('ids',       'channel==MINE');
  url.searchParams.set('metrics',   'views,likes,comments,estimatedMinutesWatched');
  url.searchParams.set('dimensions','day');
  url.searchParams.set('startDate', startDate);
  url.searchParams.set('endDate',   endDate);
  url.searchParams.set('sort',      'day');

  const res  = await fetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
  const data = await res.json();
  if (data.error) { console.warn('  Daily analytics error:', data.error.message); return; }

  const cols = (data.columnHeaders || []).map(h => h.name);
  const di = cols.indexOf('day'),
        vi = cols.indexOf('views'),
        li = cols.indexOf('likes'),
        ci = cols.indexOf('comments'),
        wi = cols.indexOf('estimatedMinutesWatched');

  for (const row of data.rows || []) {
    await pool.query(`
      INSERT INTO daily_analytics (platform, date, views, likes, comments, watch_minutes)
      VALUES ('youtube', $1, $2, $3, $4, $5)
      ON CONFLICT (platform, date) DO UPDATE SET
        views         = EXCLUDED.views,
        likes         = EXCLUDED.likes,
        comments      = EXCLUDED.comments,
        watch_minutes = EXCLUDED.watch_minutes,
        updated_at    = NOW()
    `, [row[di], row[vi]||0, row[li]||0, row[ci]||0, row[wi]||0]);
  }
  console.log(`  ✓ Daily analytics: ${(data.rows||[]).length} days synced`);
}

// ─── Subscriber snapshots ────────────────────────────────────────────────────

async function saveSubscriberSnapshot() {
  try {
    const [ytRes, igRes] = await Promise.all([
      fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${process.env.YOUTUBE_CHANNEL_ID}&key=${process.env.YOUTUBE_API_KEY}`),
      fetch(`https://graph.instagram.com/v21.0/me?fields=followers_count&access_token=${process.env.INSTAGRAM_ACCESS_TOKEN}`),
    ]);
    const yt = await ytRes.json();
    const ig = await igRes.json();
    const ytCount = parseInt(yt.items?.[0]?.statistics?.subscriberCount || 0);
    const igCount = Number(ig.followers_count || 0);
    await pool.query(
      `INSERT INTO subscriber_snapshots (platform, count) VALUES ('youtube',$1),('instagram',$2)`,
      [ytCount, igCount]
    );
    console.log(`  ✓ Subscriber snapshot (YT: ${ytCount.toLocaleString()}, IG: ${igCount.toLocaleString()})`);
  } catch (err) {
    console.warn('  Subscriber snapshot failed:', err.message);
  }
}

// ─── Main sync ───────────────────────────────────────────────────────────────

async function runSync() {
  console.log(`[${new Date().toISOString()}] Sync started`);

  try {
    const igRecords = await syncInstagram();
    await upsertRecords(igRecords);
    console.log(`  ✓ Instagram: ${igRecords.length} posts synced`);
  } catch (err) {
    console.error(`  ✗ Instagram failed: ${err.message}`);
  }

  try {
    const ytRecords = await syncYoutube();
    await upsertRecords(ytRecords);
    console.log(`  ✓ YouTube: ${ytRecords.length} videos synced`);
  } catch (err) {
    console.error(`  ✗ YouTube failed: ${err.message}`);
  }

  try {
    await syncDailyYouTubeAnalytics();
  } catch (err) {
    console.error(`  ✗ Daily analytics failed: ${err.message}`);
  }

  await saveSubscriberSnapshot();

  console.log(`[${new Date().toISOString()}] Sync complete`);
}

// ─── Schedule ────────────────────────────────────────────────────────────────

const runOnce = process.argv.includes('--once');

runSync().then(() => {
  if (runOnce) {
    pool.end();
    process.exit(0);
  }

  // Every 6 hours: midnight, 6am, noon, 6pm
  cron.schedule('0 0,6,12,18 * * *', runSync);
  console.log('Cron scheduled — runs every 6 hours (00:00, 06:00, 12:00, 18:00)');
});
