require('dotenv').config();
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
const dns = require('dns');
dns.setDefaultResultOrder('ipv4first');
const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const app = express();

// Support Vercel-Supabase integration vars, manual DATABASE_URL, or build from parts
const connectionString =
  process.env.DATABASE_URL ||
  process.env.POSTGRES_URL ||
  process.env.POSTGRES_URL_NON_POOLING ||
  (process.env.POSTGRES_HOST
    ? `postgresql://${process.env.POSTGRES_USER}:${encodeURIComponent(process.env.POSTGRES_PASSWORD)}@${process.env.POSTGRES_HOST}/${process.env.POSTGRES_DATABASE || 'postgres'}`
    : null);

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
});

pool.query(`
  CREATE TABLE IF NOT EXISTS content_analytics (
    id               BIGSERIAL PRIMARY KEY,
    platform         TEXT NOT NULL,
    post_id          TEXT NOT NULL UNIQUE,
    title            TEXT,
    thumbnail_url    TEXT,
    published_at     TIMESTAMPTZ,
    views            BIGINT DEFAULT 0,
    likes            BIGINT DEFAULT 0,
    comments         BIGINT DEFAULT 0,
    shares           BIGINT,
    saves            BIGINT,
    reach            BIGINT,
    engagement_rate  NUMERIC(6,2),
    ctr              NUMERIC(6,2),
    avg_view_duration INT,
    watch_time_minutes BIGINT,
    yt_impressions   BIGINT,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
  )
`).catch(err => console.warn('content_analytics init:', err.message));

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

app.use(express.static(path.join(__dirname, 'public')));

function pct(curr, prev) {
  const c = Number(curr), p = Number(prev);
  if (!p) return c > 0 ? 100 : 0;
  return Math.round(((c - p) / p) * 1000) / 10;
}


app.get('/api/dashboard', async (req, res) => {
  try {
    const range    = Math.min(Math.max(parseInt(req.query.range) || 30, 1), 365);
    const platform = req.query.platform || 'all';
    const sort     = ['views', 'likes', 'shares', 'engagement_rate'].includes(req.query.sort)
      ? req.query.sort : 'views';

    const hasPf = platform === 'instagram' || platform === 'youtube';
    const pf    = hasPf ? 'AND platform = $2' : '';
    const pf2   = hasPf ? 'AND platform = $3' : '';
    const a1    = hasPf ? [range, platform] : [range];
    const a2    = hasPf ? [range, range, platform] : [range, range];

    const [curr, prev, daily, posts] = await Promise.all([
      pool.query(`
        SELECT
          COALESCE(SUM(views), 0)                              AS views,
          COALESCE(SUM(reach), 0)                              AS reach,
          COALESCE(SUM(likes), 0)                              AS likes,
          COALESCE(SUM(comments), 0)                           AS comments,
          COALESCE(ROUND(AVG(engagement_rate)::numeric, 2), 0) AS er,
          COALESCE(SUM(shares), 0)                             AS shares,
          COALESCE(SUM(saves), 0)                              AS saves,
          ROUND(AVG(ctr)::numeric, 3)                          AS ctr,
          ROUND(AVG(avg_view_duration)::numeric, 0)            AS avd,
          ROUND(SUM(watch_time_minutes)::numeric, 0)           AS watch_time
        FROM content_analytics
        WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf}
      `, a1),

      pool.query(`
        SELECT
          COALESCE(SUM(views), 0)                              AS views,
          COALESCE(SUM(reach), 0)                              AS reach,
          COALESCE(SUM(likes), 0)                              AS likes,
          COALESCE(SUM(comments), 0)                           AS comments,
          COALESCE(ROUND(AVG(engagement_rate)::numeric, 2), 0) AS er,
          COALESCE(SUM(shares), 0)                             AS shares,
          COALESCE(SUM(saves), 0)                              AS saves,
          ROUND(AVG(ctr)::numeric, 3)                          AS ctr,
          ROUND(AVG(avg_view_duration)::numeric, 0)            AS avd,
          ROUND(SUM(watch_time_minutes)::numeric, 0)           AS watch_time
        FROM content_analytics
        WHERE published_at >= NOW() - INTERVAL '1 day' * ($1::int + $2::int)
          AND published_at <  NOW() - INTERVAL '1 day' * $1::int ${pf2}
      `, a2),

      // Use real daily view data for YouTube; published-date grouping for Instagram
      (hasPf && platform === 'youtube'
        ? pool.query(`
            SELECT TO_CHAR(date, 'YYYY-MM-DD') AS date,
              COALESCE(SUM(views),    0) AS views,    0 AS reach,
              COALESCE(SUM(likes),    0) AS likes,
              COALESCE(SUM(comments), 0) AS comments,
              0 AS shares, 0 AS saves,                0 AS er
            FROM daily_analytics
            WHERE platform = 'youtube'
              AND date >= NOW() - INTERVAL '1 day' * $1::int
            GROUP BY date ORDER BY date
          `, [range])
        : pool.query(`
            SELECT
              TO_CHAR(DATE_TRUNC('day', published_at), 'YYYY-MM-DD') AS date,
              COALESCE(SUM(views), 0)                              AS views,
              COALESCE(SUM(reach), 0)                              AS reach,
              COALESCE(SUM(likes), 0)                              AS likes,
              COALESCE(SUM(comments), 0)                           AS comments,
              COALESCE(SUM(COALESCE(shares, 0)), 0)                AS shares,
              COALESCE(SUM(COALESCE(saves,  0)), 0)                AS saves,
              COALESCE(ROUND(AVG(engagement_rate)::numeric, 2), 0) AS er
            FROM content_analytics
            WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf}
            GROUP BY DATE_TRUNC('day', published_at) ORDER BY date
          `, a1)),

      pool.query(`
        SELECT
          post_id, platform, title, thumbnail_url,
          published_at                       AS published_at,
          COALESCE(views, 0)                 AS views,
          COALESCE(likes, 0)                 AS likes,
          COALESCE(comments, 0)              AS comments,
          shares, saves,
          COALESCE(engagement_rate, 0)       AS engagement_rate,
          ctr, avg_view_duration, watch_time_minutes
        FROM content_analytics
        WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf}
        ORDER BY COALESCE(${sort}, 0) DESC NULLS LAST
        LIMIT 25
      `, a1),
    ]);

    const c = curr.rows[0], p = prev.rows[0];

    // Per-platform breakdown for "all" view
    let by_platform = null, daily_by_platform = null;
    if (!hasPf) {
      const [bpKpi, bpDailyYT, bpDailyIG] = await Promise.all([
        pool.query(`
          SELECT platform,
            COALESCE(SUM(views), 0)                              AS views,
            COALESCE(SUM(reach), 0)                              AS reach,
            COALESCE(SUM(likes), 0)                              AS likes,
            COALESCE(SUM(comments), 0)                           AS comments,
            COALESCE(ROUND(AVG(engagement_rate)::numeric, 2), 0) AS er,
            COALESCE(SUM(shares), 0)                             AS shares,
            COALESCE(SUM(saves), 0)                              AS saves
          FROM content_analytics
          WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int
          GROUP BY platform
        `, [range]),
        // YouTube: real daily view counts
        pool.query(`
          SELECT TO_CHAR(date, 'YYYY-MM-DD') AS date,
            COALESCE(SUM(views), 0) AS views, 0 AS reach
          FROM daily_analytics
          WHERE platform = 'youtube'
            AND date >= NOW() - INTERVAL '1 day' * $1::int
          GROUP BY date ORDER BY date
        `, [range]),
        // Instagram: grouped by publish date (best available)
        pool.query(`
          SELECT TO_CHAR(DATE_TRUNC('day', published_at), 'YYYY-MM-DD') AS date,
            COALESCE(SUM(views), 0) AS views, COALESCE(SUM(reach), 0) AS reach
          FROM content_analytics
          WHERE platform = 'instagram'
            AND published_at >= NOW() - INTERVAL '1 day' * $1::int
          GROUP BY DATE_TRUNC('day', published_at) ORDER BY date
        `, [range]),
      ]);
      by_platform = {};
      bpKpi.rows.forEach(row => { by_platform[row.platform] = row; });
      daily_by_platform = { youtube: {}, instagram: {} };
      bpDailyYT.rows.forEach(row => { daily_by_platform.youtube[row.date]   = row; });
      bpDailyIG.rows.forEach(row => { daily_by_platform.instagram[row.date] = row; });
    }

    res.json({
      kpis: {
        views:              { value: Number(c.views),      change: pct(c.views,      p.views)      },
        reach:              { value: Number(c.reach),      change: pct(c.reach,      p.reach)      },
        likes:              { value: Number(c.likes),      change: pct(c.likes,      p.likes)      },
        comments:           { value: Number(c.comments),   change: pct(c.comments,   p.comments)   },
        engagement_rate:    { value: Number(c.er),         change: pct(c.er,         p.er)         },
        shares:             { value: Number(c.shares),     change: pct(c.shares,     p.shares)     },
        saves:              { value: Number(c.saves),      change: pct(c.saves,      p.saves)      },
        ctr:                { value: c.ctr != null ? Number(c.ctr) : null,                change: pct(c.ctr,        p.ctr)        },
        avg_view_duration:  { value: c.avd != null ? Number(c.avd) : null,                change: pct(c.avd,        p.avd)        },
        watch_time_minutes: { value: c.watch_time != null ? Number(c.watch_time) : null,  change: pct(c.watch_time, p.watch_time) },
      },
      daily: daily.rows,
      posts: posts.rows,
      by_platform,
      daily_by_platform,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/followers', async (req, res) => {
  try {
    const platform = req.query.platform || 'all';
    const [igR, ytR] = await Promise.all([
      fetch(`https://graph.instagram.com/v21.0/me?fields=followers_count&access_token=${process.env.INSTAGRAM_ACCESS_TOKEN}`),
      fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${process.env.YOUTUBE_CHANNEL_ID}&key=${process.env.YOUTUBE_API_KEY}`),
    ]);
    const ig = await igR.json();
    const yt = await ytR.json();
    const igN = Number(ig.followers_count || 0);
    const ytN = parseInt(yt.items?.[0]?.statistics?.subscriberCount || 0);
    const value = platform === 'instagram' ? igN : platform === 'youtube' ? ytN : igN + ytN;
    res.json({ value, instagram: igN, youtube: ytN });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── All-time totals ───────────────────────────────────────────────────────
app.get('/api/alltime', async (req, res) => {
  try {
    const platform = req.query.platform || 'all';
    const hasPf = platform === 'instagram' || platform === 'youtube';
    const where = hasPf ? 'WHERE platform = $1' : '';
    const args  = hasPf ? [platform] : [];
    const r = await pool.query(`
      SELECT
        COALESCE(SUM(views), 0)                                        AS views,
        COALESCE(ROUND(SUM(watch_time_minutes)::numeric / 60, 0), 0)   AS watch_hours,
        COUNT(*)::int                                                  AS posts,
        COALESCE(SUM(likes), 0)                                        AS likes,
        COALESCE(SUM(comments), 0)                                     AS comments,
        MIN(published_at)                                              AS first_post
      FROM content_analytics ${where}
    `, args);
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Insights: day-of-week, trajectory, keywords ───────────────────────────
app.get('/api/insights', async (req, res) => {
  try {
    const platform = req.query.platform || 'all';
    const hasPf = platform === 'instagram' || platform === 'youtube';
    const pf   = hasPf ? 'AND platform = $1' : '';
    const args = hasPf ? [platform] : [];

    const [dowR, trajR, kwR] = await Promise.all([
      pool.query(`
        SELECT EXTRACT(DOW FROM published_at)::int AS dow,
               ROUND(AVG(views))::bigint           AS avg_views,
               COUNT(*)::int                       AS count
        FROM content_analytics WHERE 1=1 ${pf}
        GROUP BY dow ORDER BY dow
      `, args),
      pool.query(`
        SELECT TO_CHAR(published_at, 'YYYY-MM-DD') AS date,
               title, views::bigint, engagement_rate::float, platform, post_id
        FROM content_analytics WHERE 1=1 ${pf}
        ORDER BY published_at ASC
      `, args),
      pool.query(`
        SELECT title, views FROM content_analytics
        WHERE platform = 'youtube' AND title IS NOT NULL
        ORDER BY published_at DESC LIMIT 200
      `),
    ]);

    // Keyword analysis (JS-side)
    const STOP = new Set(['the','a','an','and','or','in','of','to','is','was','are','were','be','been','have','has','had','do','does','did','for','on','with','as','by','at','from','this','that','these','those','it','its','but','not','what','who','how','when','where','why','which','my','your','his','her','our','their','i','we','you','he','she','they','me','him','us','them','into','about','up','out']);
    const wm = {};
    for (const row of kwR.rows) {
      if (!row.title) continue;
      const words = [...new Set(
        row.title.toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(w => w.length > 2 && !STOP.has(w))
      )];
      for (const w of words) {
        if (!wm[w]) wm[w] = { total: 0, count: 0 };
        wm[w].total += Number(row.views);
        wm[w].count++;
      }
    }
    const keywords = Object.entries(wm)
      .filter(([, v]) => v.count >= 2)
      .map(([word, v]) => ({ word, avg_views: Math.round(v.total / v.count), count: v.count }))
      .sort((a, b) => b.avg_views - a.avg_views)
      .slice(0, 12);

    // Per-platform day-of-week for "all" view
    let dayofweek_by_platform = null;
    if (!hasPf) {
      const dowBpR = await pool.query(`
        SELECT platform,
               EXTRACT(DOW FROM published_at)::int AS dow,
               ROUND(AVG(views))::bigint            AS avg_views,
               COUNT(*)::int                        AS count
        FROM content_analytics
        GROUP BY platform, dow
        ORDER BY platform, dow
      `);
      dayofweek_by_platform = {};
      dowBpR.rows.forEach(row => {
        if (!dayofweek_by_platform[row.platform]) dayofweek_by_platform[row.platform] = {};
        dayofweek_by_platform[row.platform][row.dow] = row;
      });
    }

    res.json({ dayofweek: dowR.rows, dayofweek_by_platform, trajectory: trajR.rows, keywords });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Subscriber / follower growth history ─────────────────────────────────
app.get('/api/subscribers/history', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT platform,
             ROUND(AVG(count))::bigint AS count,
             TO_CHAR(DATE_TRUNC('day', recorded_at), 'YYYY-MM-DD') AS date
      FROM subscriber_snapshots
      GROUP BY platform, DATE_TRUNC('day', recorded_at)
      ORDER BY DATE_TRUNC('day', recorded_at)
    `);
    const map = {};
    for (const row of r.rows) {
      if (!map[row.date]) map[row.date] = { date: row.date };
      map[row.date][row.platform] = Number(row.count);
    }
    res.json(Object.values(map).sort((a, b) => a.date.localeCompare(b.date)));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Upload Checklist ──────────────────────────────────────────────────────
pool.query(`
  CREATE TABLE IF NOT EXISTS checklist_items (
    id         BIGSERIAL PRIMARY KEY,
    label      TEXT NOT NULL,
    category   TEXT NOT NULL DEFAULT 'general',
    sort_order INT  NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )
`).then(async () => {
  // Fix any stale default items
  await pool.query(`UPDATE checklist_items SET label='Title: 3 options written & best picked' WHERE label LIKE 'Title%tested%'`).catch(()=>{});
  await pool.query(`DELETE FROM checklist_items WHERE label LIKE 'Tags added%'`).catch(()=>{});

  // Seed default items if table is empty
  const { rows } = await pool.query('SELECT COUNT(*) FROM checklist_items');
  if (Number(rows[0].count) === 0) {
    const defaults = [
      ['Thumbnail designed & uploaded',       'visuals',  0],
      ['Title: 3 options written & best picked',  'metadata', 1],
      ['Description written (first 3 lines)', 'metadata', 2],
      ['Chapters / timestamps added',          'metadata', 3],
      ['End screen configured',               'engagement',5],
      ['Cards added (pinned comment, links)',  'engagement',6],
      ['Pinned comment written',              'engagement',7],
      ['Posted to Instagram / Reels',         'promotion', 8],
      ['Community post scheduled',            'promotion', 9],
      ['Email list notified',                 'promotion',10],
      ['Captions / subtitles uploaded',       'accessibility',11],
    ];
    for (const [label, category, sort_order] of defaults) {
      await pool.query('INSERT INTO checklist_items (label,category,sort_order) VALUES ($1,$2,$3)', [label,category,sort_order]);
    }
  }
}).catch(err => console.warn('checklist_items init:', err.message));

app.get('/api/checklist', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM checklist_items ORDER BY sort_order, id');
    res.json(r.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/checklist', express.json(), async (req, res) => {
  try {
    const { label, category = 'general' } = req.body;
    if (!label) return res.status(400).json({ error: 'label required' });
    const r = await pool.query(
      'INSERT INTO checklist_items (label,category) VALUES ($1,$2) RETURNING *',
      [label, category]
    );
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/checklist/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM checklist_items WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Thumbnail Gallery ─────────────────────────────────────────────────────
app.get('/api/thumbnails', async (req, res) => {
  try {
    const platform = req.query.platform || 'youtube';
    const sort     = ['views','engagement_rate','likes','published_at'].includes(req.query.sort) ? req.query.sort : 'views';
    const hasPf    = platform === 'youtube' || platform === 'instagram';
    const where    = hasPf ? `WHERE platform = '${platform}' AND thumbnail_url IS NOT NULL` : `WHERE thumbnail_url IS NOT NULL`;
    const r = await pool.query(`
      SELECT post_id, platform, title, thumbnail_url, published_at,
             views, likes, comments, engagement_rate, watch_time_minutes
      FROM content_analytics
      ${where}
      ORDER BY ${sort} DESC NULLS LAST
      LIMIT 50
    `);
    res.json(r.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── AI Idea Generator ─────────────────────────────────────────────────────
const Anthropic = require('@anthropic-ai/sdk');

app.post('/api/ideas/generate', express.json(), async (req, res) => {
  try {
    if (!process.env.ANTHROPIC_API_KEY || process.env.ANTHROPIC_API_KEY === 'your_anthropic_api_key_here') {
      return res.status(400).json({ error: 'ANTHROPIC_API_KEY not set in .env' });
    }

    const { angle = '', count = 8 } = req.body;

    // Pull context from DB
    const [topR, kwR, recentR] = await Promise.all([
      pool.query(`
        SELECT title, views, likes, engagement_rate, platform, published_at
        FROM content_analytics
        WHERE platform = 'youtube' AND title IS NOT NULL
        ORDER BY views DESC LIMIT 20
      `),
      pool.query(`
        SELECT title, views FROM content_analytics
        WHERE platform = 'youtube' AND title IS NOT NULL
        ORDER BY published_at DESC LIMIT 50
      `),
      pool.query(`
        SELECT title FROM content_analytics
        WHERE platform = 'youtube' AND title IS NOT NULL
        ORDER BY published_at DESC LIMIT 10
      `),
    ]);

    const topTitles = topR.rows.map(r =>
      `"${r.title}" — ${Number(r.views).toLocaleString()} views, ${r.engagement_rate}% ER`
    ).join('\n');

    const recentTitles = recentR.rows.map(r => `"${r.title}"`).join('\n');

    // Extract keywords from recent titles
    const STOP = new Set(['the','a','an','and','or','in','of','to','is','was','are','for','on','with','by','from','this','that','my','your','his','her','i','we','you']);
    const kwMap = {};
    for (const row of kwR.rows) {
      const words = (row.title||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(w => w.length > 2 && !STOP.has(w));
      for (const w of [...new Set(words)]) {
        if (!kwMap[w]) kwMap[w] = { total: 0, count: 0 };
        kwMap[w].total += Number(row.views);
        kwMap[w].count++;
      }
    }
    const topKw = Object.entries(kwMap)
      .filter(([,v]) => v.count >= 2)
      .sort(([,a],[,b]) => (b.total/b.count) - (a.total/a.count))
      .slice(0, 15)
      .map(([w,v]) => `${w} (avg ${Math.round(v.total/v.count).toLocaleString()} views)`)
      .join(', ');

    const prompt = `You are a YouTube content strategist specializing in biblical history and historical analysis channels.

CHANNEL CONTEXT:
- Channel: Nils Glenn — biblical history, ancient civilizations, historical deep-dives
- Audience: Christians, history enthusiasts, people interested in archaeology and ancient world
- Style: Educational, documentary-style, narrative-driven

TOP PERFORMING VIDEOS (by views):
${topTitles}

HIGH-PERFORMING KEYWORDS: ${topKw}

RECENT UPLOADS (avoid direct repeats):
${recentTitles}

${angle ? `USER'S FOCUS/ANGLE: ${angle}\n` : ''}

Generate exactly ${count} original, high-potential YouTube video ideas for this channel. Each idea should:
- Have a compelling, curiosity-driven title (not clickbait, but genuinely interesting)
- Use proven keywords where natural
- Be distinct from recent uploads
- Include a 1-2 sentence hook explaining why it'll perform well

Format each idea EXACTLY like this (no markdown headers, just numbered):

1. [TITLE]
Hook: [why this will resonate and perform well — specific to the channel's audience]
Angle: [unique storytelling angle or framing]

2. [TITLE]
Hook: [...]
Angle: [...]

(continue for all ${count} ideas)`;

    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    // Stream the response back to the browser
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Transfer-Encoding', 'chunked');
    res.setHeader('Cache-Control', 'no-cache');

    const stream = await client.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 4000,
      thinking: { type: 'adaptive' },
      messages: [{ role: 'user', content: prompt }],
      stream: true,
    });

    for await (const event of stream) {
      if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
        res.write(event.delta.text);
      }
    }
    res.end();

  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
    else res.end('\n\n[Error: ' + err.message + ']');
  }
});

// ── Keyword Gap ───────────────────────────────────────────────────────────
app.get('/api/keyword-gap', async (req, res) => {
  try {
    // Top-performing videos (top 25% by views) — extract their title words
    const r = await pool.query(`
      SELECT title, views FROM content_analytics
      WHERE platform = 'youtube' AND title IS NOT NULL
      ORDER BY views DESC
    `);
    const rows = r.rows;
    if (rows.length < 4) return res.json({ gap: [], used: [] });

    const STOP = new Set(['the','a','an','and','or','in','of','to','is','was','are','were','be','been','have','has','had','do','does','did','for','on','with','as','by','at','from','this','that','these','those','it','its','but','not','what','who','how','when','where','why','which','my','your','his','her','our','their','i','we','you','he','she','they','me','him','us','them','into','about','up','out','vs','part']);

    const topN    = Math.max(1, Math.ceil(rows.length * 0.25));
    const topRows = rows.slice(0, topN);
    const botRows = rows.slice(topN);

    function wordSet(arr) {
      const map = {};
      for (const row of arr) {
        const words = (row.title || '').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/)
          .filter(w => w.length > 2 && !STOP.has(w));
        for (const w of [...new Set(words)]) {
          if (!map[w]) map[w] = { count: 0, totalViews: 0 };
          map[w].count++;
          map[w].totalViews += Number(row.views);
        }
      }
      return map;
    }

    const topWords = wordSet(topRows);
    const allWords = wordSet(rows);
    const botSet   = new Set(botRows.flatMap(r =>
      (r.title||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(w => w.length > 2 && !STOP.has(w))
    ));

    // Recent titles (last 10) — words you're currently using
    const recentTitles = rows.slice(0, 10);
    const recentWords  = new Set(recentTitles.flatMap(r =>
      (r.title||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(w => w.length > 2 && !STOP.has(w))
    ));

    // Gap = words in top performers but NOT in your recent 10 titles
    const gap = Object.entries(topWords)
      .filter(([w]) => !recentWords.has(w))
      .map(([word, v]) => ({
        word,
        avgViews:   Math.round(v.totalViews / v.count),
        topCount:   v.count,
        usedInBot:  botSet.has(word),
        allAvg:     allWords[word] ? Math.round(allWords[word].totalViews / allWords[word].count) : 0,
      }))
      .sort((a, b) => b.avgViews - a.avgViews)
      .slice(0, 15);

    // Used = words you use often but with below-average performance
    const avgViews = rows.reduce((s,r) => s + Number(r.views), 0) / rows.length;
    const used = Object.entries(allWords)
      .filter(([w, v]) => v.count >= 2 && (v.totalViews / v.count) < avgViews * 0.7)
      .map(([word, v]) => ({ word, avgViews: Math.round(v.totalViews / v.count), count: v.count }))
      .sort((a, b) => a.avgViews - b.avgViews)
      .slice(0, 10);

    res.json({ gap, used, avgViews: Math.round(avgViews) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Comments Explorer ─────────────────────────────────────────────────────
app.get('/api/comments', async (req, res) => {
  try {
    const apiKey    = process.env.YOUTUBE_API_KEY;
    const channelId = process.env.YOUTUBE_CHANNEL_ID;
    const maxResults = Math.min(parseInt(req.query.limit) || 100, 100);
    const order      = ['time', 'relevance'].includes(req.query.order) ? req.query.order : 'time';
    const videoId    = req.query.videoId || null;

    let url;
    if (videoId) {
      url = `https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId=${videoId}&maxResults=${maxResults}&order=${order}&key=${apiKey}`;
    } else {
      url = `https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&allThreadsRelatedToChannelId=${channelId}&maxResults=${maxResults}&order=${order}&key=${apiKey}`;
    }

    const data = await fetch(url).then(r => r.json());
    if (data.error) return res.status(400).json({ error: data.error.message });

    const comments = (data.items || []).map(item => {
      const top = item.snippet.topLevelComment.snippet;
      return {
        id:          item.id,
        videoId:     top.videoId,
        author:      top.authorDisplayName,
        authorPic:   top.authorProfileImageUrl,
        text:        top.textDisplay,
        likes:       top.likeCount || 0,
        replyCount:  item.snippet.totalReplyCount || 0,
        publishedAt: top.publishedAt,
      };
    });

    res.json({ comments, nextPageToken: data.nextPageToken || null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Content Pipeline ──────────────────────────────────────────────────────
pool.query(`
  CREATE TABLE IF NOT EXISTS pipeline_cards (
    id          BIGSERIAL PRIMARY KEY,
    stage       TEXT NOT NULL DEFAULT 'idea',
    title       TEXT NOT NULL,
    notes       TEXT,
    platform    TEXT,
    target_date DATE,
    sort_order  INT  NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
  )
`).catch(err => console.warn('pipeline_cards init:', err.message));

app.get('/api/pipeline', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT id, stage, title, notes, platform,
             TO_CHAR(target_date, 'YYYY-MM-DD') AS target_date,
             sort_order, created_at
      FROM pipeline_cards
      ORDER BY stage, sort_order, created_at
    `);
    res.json(r.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/pipeline', express.json(), async (req, res) => {
  try {
    const { title, stage = 'idea', notes, platform, target_date } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const r = await pool.query(`
      INSERT INTO pipeline_cards (title, stage, notes, platform, target_date)
      VALUES ($1, $2, $3, $4, $5) RETURNING *
    `, [title, stage, notes || null, platform || null, target_date || null]);
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/pipeline/:id', express.json(), async (req, res) => {
  try {
    const { title, stage, notes, platform, target_date, sort_order } = req.body;
    const r = await pool.query(`
      UPDATE pipeline_cards
      SET title       = COALESCE($1, title),
          stage       = COALESCE($2, stage),
          notes       = COALESCE($3, notes),
          platform    = COALESCE($4, platform),
          target_date = COALESCE($5::date, target_date),
          sort_order  = COALESCE($6, sort_order),
          updated_at  = NOW()
      WHERE id = $7 RETURNING *
    `, [title, stage, notes, platform, target_date || null, sort_order ?? null, req.params.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/pipeline/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM pipeline_cards WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Dashboard → http://localhost:${PORT}`));
