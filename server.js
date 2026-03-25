require('dotenv').config();
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
const dns = require('dns');
dns.setDefaultResultOrder('ipv4first');
const express = require('express');
const { Client: PgClient } = require('pg');

// Serverless-safe DB helper: one fresh connection per query, always closed after.
// Avoids "Max client connections reached" from pooled connections that linger.
const pool = {
  query: async (text, params) => {
    const client = new PgClient({ connectionString, ssl: { rejectUnauthorized: false } });
    await client.connect();
    try {
      return await client.query(text, params);
    } finally {
      await client.end().catch(() => {});
    }
  }
};
const path = require('path');
const stripe = process.env.STRIPE_SECRET_KEY ? require('stripe')(process.env.STRIPE_SECRET_KEY) : null;

const app = express();

// Support Vercel-Supabase integration vars, manual DATABASE_URL, or build from parts
const connectionString =
  process.env.DATABASE_URL ||
  process.env.POSTGRES_URL ||
  process.env.POSTGRES_URL_NON_POOLING ||
  (process.env.POSTGRES_HOST
    ? `postgresql://${process.env.POSTGRES_USER}:${encodeURIComponent(process.env.POSTGRES_PASSWORD)}@${process.env.POSTGRES_HOST}/${process.env.POSTGRES_DATABASE || 'postgres'}`
    : null);


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
  CREATE TABLE IF NOT EXISTS goals (
    id           SERIAL PRIMARY KEY,
    label        TEXT NOT NULL,
    metric       TEXT NOT NULL,
    target_value BIGINT NOT NULL,
    target_date  DATE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
  )
`).catch(err => console.warn('goals init:', err.message));

pool.query(`
  CREATE TABLE IF NOT EXISTS competitors (
    id           SERIAL PRIMARY KEY,
    channel_id   TEXT NOT NULL UNIQUE,
    name         TEXT,
    handle       TEXT,
    avatar_url   TEXT,
    added_at     TIMESTAMPTZ DEFAULT NOW()
  )
`).catch(err => console.warn('competitors init:', err.message));

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

// Add cached stats columns to competitors
pool.query(`
  ALTER TABLE competitors
    ADD COLUMN IF NOT EXISTS avg_recent_views BIGINT,
    ADD COLUMN IF NOT EXISTS posts_per_month  NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS stats_updated_at TIMESTAMPTZ
`).catch(() => {});

// Add channel_id to content_analytics
pool.query(`ALTER TABLE content_analytics ADD COLUMN IF NOT EXISTS channel_id TEXT`)
  .then(() => {
    // Backfill existing rows that belong to the owner's channel
    const ownerChannel = (process.env.YOUTUBE_CHANNEL_ID || '').replace(/[^a-zA-Z0-9_-]/g, '');
    if (ownerChannel) {
      pool.query(`UPDATE content_analytics SET channel_id = $1 WHERE channel_id IS NULL AND platform = 'youtube'`, [ownerChannel])
        .catch(() => {});
    }
  })
  .catch(() => {});

app.use(express.static(path.join(__dirname, 'public'), { extensions: ['html'] }));

// Trim newlines that sneak in from `echo "..." | vercel env add`
const GOOGLE_CLIENT_ID     = (process.env.GOOGLE_CLIENT_ID     || '').trim();
const GOOGLE_CLIENT_SECRET = (process.env.GOOGLE_CLIENT_SECRET || '').trim();

function getChannelCond() {
  const id = (process.env.YOUTUBE_CHANNEL_ID || '').replace(/[^a-zA-Z0-9_-]/g, '');
  return id ? `AND (channel_id = '${id}' OR channel_id IS NULL)` : '';
}

function pct(curr, prev) {
  const c = Number(curr), p = Number(prev);
  if (!p) return c > 0 ? 100 : 0;
  return Math.round(((c - p) / p) * 1000) / 10;
}


app.get('/api/dashboard', async (req, res) => {
  try {
    const range    = Math.min(Math.max(parseInt(req.query.range) || 30, 1), 365);
    const platform = req.query.platform || 'all';
    const sort     = ['views', 'likes', 'shares', 'engagement_rate', 'published_at'].includes(req.query.sort)
      ? req.query.sort : 'views';

    const chCond = getChannelCond();

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
        WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf} ${chCond}
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
          AND published_at <  NOW() - INTERVAL '1 day' * $1::int ${pf2} ${chCond}
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
            WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf} ${chCond}
            GROUP BY DATE_TRUNC('day', published_at) ORDER BY date
          `, a1)),

      pool.query(`
        WITH avgs AS (
          SELECT
            AVG(NULLIF(COALESCE(views, 0), 0))           AS avg_views,
            AVG(NULLIF(COALESCE(engagement_rate, 0), 0)) AS avg_er
          FROM content_analytics
          WHERE 1=1 ${pf} ${chCond}
        )
        SELECT
          ca.post_id, ca.platform, ca.title, ca.thumbnail_url, ca.published_at,
          COALESCE(ca.views, 0)           AS views,
          COALESCE(ca.likes, 0)           AS likes,
          COALESCE(ca.comments, 0)        AS comments,
          ca.shares, ca.saves,
          COALESCE(ca.engagement_rate, 0) AS engagement_rate,
          ca.ctr, ca.avg_view_duration, ca.watch_time_minutes,
          LEAST(100, GREATEST(1, ROUND(
            (COALESCE(ca.views, 0)::numeric / NULLIF(avgs.avg_views, 0) * 35
            + COALESCE(ca.engagement_rate, 0)::numeric / NULLIF(avgs.avg_er, 0) * 15)
          )::int)) AS score
        FROM content_analytics ca, avgs
        WHERE ca.published_at >= NOW() - INTERVAL '1 day' * $1::int ${pf} ${chCond}
        ORDER BY ${sort === 'published_at' ? 'ca.published_at' : `COALESCE(ca.${sort}, 0)`} DESC NULLS LAST
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
          WHERE published_at >= NOW() - INTERVAL '1 day' * $1::int ${chCond}
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

app.get('/api/latest-video', async (req, res) => {
  try {
    const chCond = getChannelCond();
    const r = await pool.query(`
      WITH avgs AS (
        SELECT
          AVG(NULLIF(COALESCE(views, 0), 0))           AS avg_views,
          AVG(NULLIF(COALESCE(engagement_rate, 0), 0)) AS avg_er
        FROM content_analytics
        WHERE platform = 'youtube' ${chCond}
      )
      SELECT
        ca.post_id, ca.title, ca.thumbnail_url, ca.published_at,
        COALESCE(ca.views, 0)           AS views,
        COALESCE(ca.likes, 0)           AS likes,
        COALESCE(ca.comments, 0)        AS comments,
        ca.avg_view_duration, ca.watch_time_minutes, ca.ctr,
        COALESCE(ca.engagement_rate, 0) AS engagement_rate,
        LEAST(100, GREATEST(1, ROUND(
          (COALESCE(ca.views, 0)::numeric / NULLIF(avgs.avg_views, 0) * 35
          + COALESCE(ca.engagement_rate, 0)::numeric / NULLIF(avgs.avg_er, 0) * 15)
        )::int)) AS score
      FROM content_analytics ca, avgs
      WHERE ca.platform = 'youtube' ${chCond}
      ORDER BY ca.published_at DESC NULLS LAST
      LIMIT 1
    `);
    if (!r.rows.length) return res.json(null);
    res.json(r.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/followers', async (req, res) => {
  try {
    const platform  = req.query.platform || 'all';
    const ytChannel = (process.env.YOUTUBE_CHANNEL_ID || '').replace(/[^a-zA-Z0-9_-]/g, '');
    const [igR, ytRaw] = await Promise.all([
      fetch(`https://graph.instagram.com/v21.0/me?fields=followers_count&access_token=${process.env.INSTAGRAM_ACCESS_TOKEN}`),
      ytChannel ? fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${ytChannel}&key=${process.env.YOUTUBE_API_KEY}`).then(r => r.json()) : Promise.resolve({}),
    ]);
    const ig = await igR.json();
    const yt = ytRaw;
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
    const chCond = getChannelCond();
    const where = `WHERE 1=1 ${hasPf ? 'AND platform = $1' : ''} ${chCond}`;
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
    const pf     = hasPf ? 'AND platform = $1' : '';
    const args   = hasPf ? [platform] : [];
    const chCond = getChannelCond();

    const [dowR, trajR, kwR, hourR] = await Promise.all([
      pool.query(`
        SELECT EXTRACT(DOW FROM published_at)::int AS dow,
               ROUND(AVG(views))::bigint           AS avg_views,
               COUNT(*)::int                       AS count
        FROM content_analytics WHERE 1=1 ${pf} ${chCond}
        GROUP BY dow ORDER BY dow
      `, args),
      pool.query(`
        SELECT TO_CHAR(published_at, 'YYYY-MM-DD') AS date,
               title, views::bigint, engagement_rate::float, platform, post_id
        FROM content_analytics WHERE 1=1 ${pf} ${chCond}
        ORDER BY published_at ASC
      `, args),
      pool.query(`
        SELECT title, views FROM content_analytics
        WHERE platform = 'youtube' AND title IS NOT NULL ${chCond}
        ORDER BY published_at DESC LIMIT 200
      `),
      pool.query(`
        SELECT EXTRACT(DOW FROM published_at)::int  AS dow,
               EXTRACT(HOUR FROM published_at)::int AS hour,
               ROUND(AVG(views))::bigint            AS avg_views,
               COUNT(*)::int                        AS count
        FROM content_analytics WHERE 1=1 ${pf} ${chCond}
        GROUP BY dow, hour ORDER BY dow, hour
      `, args),
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
        FROM content_analytics WHERE 1=1 ${chCond}
        GROUP BY platform, dow
        ORDER BY platform, dow
      `);
      dayofweek_by_platform = {};
      dowBpR.rows.forEach(row => {
        if (!dayofweek_by_platform[row.platform]) dayofweek_by_platform[row.platform] = {};
        dayofweek_by_platform[row.platform][row.dow] = row;
      });
    }

    res.json({ dayofweek: dowR.rows, dayofweek_by_platform, trajectory: trajR.rows, keywords, hour_heatmap: hourR.rows });
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
    const chCond   = getChannelCond();
    const pfCond   = hasPf ? `AND platform = '${platform}'` : '';
    const r = await pool.query(`
      SELECT post_id, platform, title, thumbnail_url, published_at,
             views, likes, comments, engagement_rate, watch_time_minutes
      FROM content_analytics
      WHERE thumbnail_url IS NOT NULL ${pfCond} ${chCond}
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
      model: 'claude-haiku-4-5',
      max_tokens: 4000,
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
    const channelId = (process.env.YOUTUBE_CHANNEL_ID || '').replace(/[^a-zA-Z0-9_-]/g, '');
    if (!channelId) return res.status(400).json({ error: 'YOUTUBE_CHANNEL_ID not configured' });
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

    const stripYTHtml = s => (s || '').replace(/<[^>]*>/g, ' ').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&apos;/g,"'").replace(/&nbsp;/g,' ').replace(/&#(\d+);/g,(_,n)=>String.fromCharCode(Number(n))).replace(/\s+/g,' ').trim();
    const comments = (data.items || []).map(item => {
      const top = item.snippet.topLevelComment.snippet;
      return {
        id:          item.id,
        videoId:     top.videoId,
        author:      top.authorDisplayName,
        authorPic:   top.authorProfileImageUrl,
        text:        stripYTHtml(top.textDisplay || top.textOriginal || ''),
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

// ── Sync helpers ──────────────────────────────────────────────────────────
async function getYTAccessToken(refreshToken) {
  // Uses the provided refresh token, or falls back to the owner's env var
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id:     GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      refresh_token: refreshToken || process.env.GOOGLE_REFRESH_TOKEN,
      grant_type:    'refresh_token',
    }),
  });
  const d = await r.json();
  if (!d.access_token) throw new Error('Could not refresh YouTube access token: ' + JSON.stringify(d));
  return d.access_token;
}


async function syncYouTube(log, userChannelId, userRefreshToken) {
  const apiKey    = process.env.YOUTUBE_API_KEY;
  const channelId = userChannelId || process.env.YOUTUBE_CHANNEL_ID;
  if (!apiKey || !channelId) throw new Error('Missing YOUTUBE_API_KEY or YOUTUBE_CHANNEL_ID');

  log('Getting YouTube access token…');
  const accessToken = await getYTAccessToken(userRefreshToken || null);

  // 1. Get uploads playlist ID
  log('Fetching channel uploads playlist…');
  const chR = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=${channelId}&key=${apiKey}`);
  const chData = await chR.json();
  const uploadsId = chData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  if (!uploadsId) throw new Error('Could not find uploads playlist');

  // 2. Paginate through uploads playlist (up to 200 videos)
  log('Fetching video list…');
  const videoIds = [];
  let pageToken = '';
  while (videoIds.length < 200) {
    const url = `https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=${uploadsId}&maxResults=50${pageToken ? '&pageToken=' + pageToken : ''}&key=${apiKey}`;
    const plR  = await fetch(url);
    const plData = await plR.json();
    (plData.items || []).forEach(i => videoIds.push(i.contentDetails.videoId));
    if (!plData.nextPageToken) break;
    pageToken = plData.nextPageToken;
  }
  log(`Found ${videoIds.length} videos.`);

  // 3. Batch fetch video statistics + snippet (50 at a time)
  const videoDetails = {};
  for (let i = 0; i < videoIds.length; i += 50) {
    const batch = videoIds.slice(i, i + 50);
    const vR = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet,contentDetails&id=${batch.join(',')}&key=${apiKey}`);
    const vData = await vR.json();
    (vData.items || []).forEach(v => { videoDetails[v.id] = v; });
  }
  log(`Fetched details for ${Object.keys(videoDetails).length} videos.`);

  // 4. Fetch YouTube Analytics (per-video metrics, all time)
  const today  = new Date().toISOString().split('T')[0];
  const analyticsUrl = `https://youtubeanalytics.googleapis.com/v2/reports?ids=channel%3D%3DMINE&dimensions=video&metrics=views,estimatedMinutesWatched,averageViewDuration,impressions,impressionClickThroughRate&startDate=2020-01-01&endDate=${today}&maxResults=200&sort=-views`;
  const anR = await fetch(analyticsUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
  const anData = await anR.json();

  const analyticsMap = {};
  if (anData.rows) {
    anData.rows.forEach(row => {
      // columns: video, views, estimatedMinutesWatched, averageViewDuration, impressions, impressionClickThroughRate
      analyticsMap[row[0]] = {
        views:               Number(row[1]),
        watch_time_minutes:  Number(row[2]),
        avg_view_duration:   Math.round(Number(row[3])),
        yt_impressions:      Number(row[4]),
        ctr:                 Number((row[5] * 100).toFixed(3)),
      };
    });
  }
  log(`Got analytics for ${Object.keys(analyticsMap).length} videos.`);

  // 5. Upsert into content_analytics
  let upserted = 0;
  for (const [vid, detail] of Object.entries(videoDetails)) {
    const an   = analyticsMap[vid] || {};
    const stat = detail.statistics || {};
    const snip = detail.snippet    || {};
    const thumb = snip.thumbnails?.maxres?.url || snip.thumbnails?.high?.url || snip.thumbnails?.medium?.url || null;
    const publishedAt = snip.publishedAt || null;
    const views   = an.views          ?? Number(stat.viewCount  || 0);
    const likes   = Number(stat.likeCount    || 0);
    const comments = Number(stat.commentCount || 0);
    const er = views > 0 ? Number(((likes + comments) / views * 100).toFixed(2)) : 0;

    await pool.query(`
      INSERT INTO content_analytics
        (platform, post_id, channel_id, title, thumbnail_url, published_at, views, likes, comments,
         engagement_rate, ctr, avg_view_duration, watch_time_minutes, yt_impressions, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
      ON CONFLICT (post_id) DO UPDATE SET
        channel_id=EXCLUDED.channel_id,
        title=EXCLUDED.title, thumbnail_url=EXCLUDED.thumbnail_url,
        views=EXCLUDED.views, likes=EXCLUDED.likes, comments=EXCLUDED.comments,
        engagement_rate=EXCLUDED.engagement_rate, ctr=EXCLUDED.ctr,
        avg_view_duration=EXCLUDED.avg_view_duration,
        watch_time_minutes=EXCLUDED.watch_time_minutes,
        yt_impressions=EXCLUDED.yt_impressions, updated_at=NOW()
    `, ['youtube', vid, channelId, snip.title, thumb, publishedAt,
        views, likes, comments, er,
        an.ctr ?? null, an.avg_view_duration ?? null,
        an.watch_time_minutes ?? null, an.yt_impressions ?? null]);
    upserted++;
  }

  // 6. Fetch daily analytics (last 90 days)
  const d90 = new Date(); d90.setDate(d90.getDate() - 90);
  const startDate = d90.toISOString().split('T')[0];
  const dailyUrl = `https://youtubeanalytics.googleapis.com/v2/reports?ids=channel%3D%3DMINE&dimensions=day&metrics=views,likes,comments,estimatedMinutesWatched&startDate=${startDate}&endDate=${today}`;
  const dailyR = await fetch(dailyUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
  const dailyData = await dailyR.json();
  if (dailyData.rows) {
    for (const row of dailyData.rows) {
      await pool.query(`
        INSERT INTO daily_analytics (platform, date, views, likes, comments, watch_minutes)
        VALUES ('youtube',$1,$2,$3,$4,$5)
        ON CONFLICT (platform, date) DO UPDATE SET
          views=EXCLUDED.views, likes=EXCLUDED.likes,
          comments=EXCLUDED.comments, watch_minutes=EXCLUDED.watch_minutes, updated_at=NOW()
      `, [row[0], row[1], row[2], row[3], row[4]]);
    }
  }

  // 7. Subscriber snapshot
  const subR = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${channelId}&key=${apiKey}`);
  const subData = await subR.json();
  const subs = parseInt(subData.items?.[0]?.statistics?.subscriberCount || 0);
  if (subs > 0) {
    await pool.query(`INSERT INTO subscriber_snapshots (platform, count) VALUES ('youtube', $1)`, [subs]);
  }

  log(`YouTube sync complete: ${upserted} videos upserted.`);
  return { upserted, subscribers: subs };
}

async function syncInstagram(log) {
  const token = process.env.INSTAGRAM_ACCESS_TOKEN;
  if (!token) throw new Error('Missing INSTAGRAM_ACCESS_TOKEN');

  log('Fetching Instagram media list…');
  const mediaR = await fetch(`https://graph.instagram.com/v21.0/me/media?fields=id,caption,media_type,timestamp,thumbnail_url,media_url&limit=50&access_token=${token}`);
  const mediaData = await mediaR.json();
  if (mediaData.error) throw new Error('Instagram media: ' + mediaData.error.message);

  const items = mediaData.data || [];
  log(`Found ${items.length} Instagram posts. Fetching insights…`);

  let upserted = 0;
  for (const media of items) {
    try {
      const insR = await fetch(`https://graph.instagram.com/v21.0/${media.id}/insights?metric=impressions,reach,likes,comments,shares,saved&period=lifetime&access_token=${token}`);
      const insData = await insR.json();

      const ins = {};
      (insData.data || []).forEach(m => { ins[m.name] = m.values?.[0]?.value ?? m.value ?? 0; });

      const views    = Number(ins.impressions || 0);
      const reach    = Number(ins.reach       || 0);
      const likes    = Number(ins.likes       || 0);
      const comments = Number(ins.comments    || 0);
      const shares   = Number(ins.shares      || 0);
      const saves    = Number(ins.saved       || 0);
      const er       = views > 0 ? Number(((likes + comments + shares + saves) / views * 100).toFixed(2)) : 0;
      const thumb    = media.thumbnail_url || media.media_url || null;

      await pool.query(`
        INSERT INTO content_analytics
          (platform, post_id, title, thumbnail_url, published_at, views, reach, likes, comments, shares, saves, engagement_rate, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
        ON CONFLICT (post_id) DO UPDATE SET
          title=EXCLUDED.title, thumbnail_url=EXCLUDED.thumbnail_url,
          views=EXCLUDED.views, reach=EXCLUDED.reach,
          likes=EXCLUDED.likes, comments=EXCLUDED.comments,
          shares=EXCLUDED.shares, saves=EXCLUDED.saves,
          engagement_rate=EXCLUDED.engagement_rate, updated_at=NOW()
      `, ['instagram', media.id, media.caption?.substring(0, 300) || null, thumb,
          media.timestamp, views, reach, likes, comments, shares, saves, er]);
      upserted++;
    } catch(e) {
      log(`Skipped ${media.id}: ${e.message}`);
    }
  }

  // Instagram follower snapshot
  const folR = await fetch(`https://graph.instagram.com/v21.0/me?fields=followers_count&access_token=${token}`);
  const folData = await folR.json();
  const followers = Number(folData.followers_count || 0);
  if (followers > 0) {
    await pool.query(`INSERT INTO subscriber_snapshots (platform, count) VALUES ('instagram', $1)`, [followers]);
  }

  log(`Instagram sync complete: ${upserted} posts upserted.`);
  return { upserted, followers };
}

app.post('/api/sync', express.json(), async (req, res) => {
  const platform = req.body?.platform || 'all';
  const logs = [];
  const log = msg => { logs.push(msg); console.log('[sync]', msg); };

  try {
    const results = {};
    if (platform === 'all' || platform === 'youtube') {
      try { results.youtube = await syncYouTube(log, null, null); }
      catch(e) { results.youtube = { error: e.message }; log('YouTube error: ' + e.message); }
    }
    if (platform === 'all' || platform === 'instagram') {
      try { results.instagram = await syncInstagram(log); }
      catch(e) { results.instagram = { error: e.message }; log('Instagram error: ' + e.message); }
    }
    res.json({ ok: true, results, logs });
  } catch(err) {
    res.status(500).json({ ok: false, error: err.message, logs });
  }
});

// ── Goals ─────────────────────────────────────────────────────────────────
app.get('/api/goals', async (req, res) => {
  try {
    const r = await pool.query(`SELECT * FROM goals ORDER BY created_at ASC`);
    res.json(r.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/goals', express.json(), async (req, res) => {
  try {
    const { label, metric, target_value, target_date } = req.body;
    if (!label || !metric || !target_value) return res.status(400).json({ error: 'Missing fields' });
    const r = await pool.query(
      `INSERT INTO goals (label, metric, target_value, target_date) VALUES ($1,$2,$3,$4) RETURNING *`,
      [label, metric, Number(target_value), target_date || null]
    );
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/goals/:id', async (req, res) => {
  try {
    await pool.query(`DELETE FROM goals WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Content Patterns ───────────────────────────────────────────────────────
app.get('/api/patterns', async (req, res) => {
  try {
    const platform = req.query.platform || 'youtube';
    const pf = (platform === 'youtube' || platform === 'instagram') ? `AND platform = $1` : '';
    const args = pf ? [platform] : [];
    const chCond = getChannelCond();

    const r = await pool.query(`
      SELECT title, views, engagement_rate, shares, saves,
             EXTRACT(DOW FROM published_at)::int  AS dow,
             EXTRACT(HOUR FROM published_at)::int AS hour,
             EXTRACT(EPOCH FROM (NOW() - published_at)) / 86400 AS age_days
      FROM content_analytics
      WHERE title IS NOT NULL ${pf} ${chCond}
      ORDER BY views DESC
    `, args);

    const rows = r.rows.map(r => ({ ...r, views: Number(r.views), er: Number(r.engagement_rate) }));
    if (rows.length < 5) return res.json({ insufficient_data: true });

    const cutoff = Math.ceil(rows.length * 0.2);
    const top    = rows.slice(0, cutoff);
    const all    = rows;

    const avg    = arr => arr.reduce((s, v) => s + v, 0) / (arr.length || 1);
    const mode   = arr => { const m = {}; arr.forEach(v => m[v] = (m[v]||0)+1); return +Object.entries(m).sort((a,b)=>b[1]-a[1])[0][0]; };

    const DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    const STOP = new Set(['the','a','an','and','or','in','of','to','is','was','are','were','be','been','have','has','had','do','does','did','for','on','with','as','by','at','from','this','that','these','those','it','its','but','not','what','who','how','when','where','why','which','my','your','his','her','our','their','i','we','you','he','she','they','me','him','us','them','into','about','up','out','its']);

    function topWords(posts, n) {
      const wm = {};
      posts.forEach(p => {
        const words = [...new Set((p.title||'').toLowerCase().replace(/[^a-z0-9 ]/g,' ').split(/\s+/).filter(w => w.length > 2 && !STOP.has(w)))];
        words.forEach(w => wm[w] = (wm[w]||0)+1);
      });
      return Object.entries(wm).sort((a,b)=>b[1]-a[1]).slice(0,n).map(([w])=>w);
    }

    function titleWordCount(posts) { return avg(posts.map(p => (p.title||'').split(/\s+/).filter(Boolean).length)); }

    const topDow   = mode(top.map(p => p.dow));
    const topHour  = mode(top.map(p => p.hour));
    const allAvgEr = avg(all.map(p => p.er));
    const topAvgEr = avg(top.map(p => p.er));
    const allAvgV  = avg(all.map(p => p.views));
    const topAvgV  = avg(top.map(p => p.views));
    const topWc    = titleWordCount(top);
    const allWc    = titleWordCount(all);
    const topKw    = topWords(top, 5);
    const allKw    = topWords(all, 5);

    const hourLabel = h => h === 0 ? '12am' : h < 12 ? `${h}am` : h === 12 ? '12pm' : `${h-12}pm`;

    const insights = [
      `Your top 20% of videos average ${Math.round(topAvgV).toLocaleString()} views — ${Math.round((topAvgV/allAvgV - 1)*100)}% above your channel average of ${Math.round(allAvgV).toLocaleString()}.`,
      `Top performers are most often posted on ${DAYS[topDow]} at ${hourLabel(topHour)}.`,
      `Top video titles average ${topWc.toFixed(1)} words — ${topWc > allWc ? 'longer' : 'shorter'} than your overall average of ${allWc.toFixed(1)} words.`,
      topAvgEr > allAvgEr
        ? `Top videos have ${topAvgEr.toFixed(1)}% engagement rate — ${((topAvgEr/allAvgEr-1)*100).toFixed(0)}% higher than your avg of ${allAvgEr.toFixed(1)}%.`
        : `Top videos by views have ${topAvgEr.toFixed(1)}% engagement rate, similar to your channel avg of ${allAvgEr.toFixed(1)}%.`,
      topKw.length ? `Keywords appearing most in your top videos: ${topKw.map(w=>`"${w}"`).join(', ')}.` : null,
      allKw.filter(w => !topKw.includes(w)).length
        ? `Keywords common across all videos but less in top performers: ${allKw.filter(w=>!topKw.includes(w)).slice(0,3).map(w=>`"${w}"`).join(', ')}.`
        : null,
    ].filter(Boolean);

    res.json({
      total: all.length,
      top_count: top.length,
      insights,
      top_avg_views: Math.round(topAvgV),
      all_avg_views: Math.round(allAvgV),
      top_day: DAYS[topDow],
      top_hour: hourLabel(topHour),
      top_keywords: topKw,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Competitors ────────────────────────────────────────────────────────────
async function fetchChannelStats(channelId) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  const url = `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,brandingSettings&id=${channelId}&key=${apiKey}`;
  const r    = await fetch(url);
  const data = await r.json();
  const item = data.items?.[0];
  if (!item) throw new Error('Channel not found');

  // Get recent videos for avg views calculation
  const uploadsId = item.contentDetails?.relatedPlaylists?.uploads;
  let avgViews = null, postsPerMonth = null;
  if (uploadsId) {
    const plR = await fetch(`https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=${uploadsId}&maxResults=20&key=${apiKey}`);
    const plData = await plR.json();
    const vids = (plData.items || []).map(i => i.contentDetails.videoId);
    if (vids.length) {
      const vsR = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${vids.join(',')}&key=${apiKey}`);
      const vsData = await vsR.json();
      const items = vsData.items || [];
      if (items.length) {
        avgViews = Math.round(items.reduce((s, v) => s + Number(v.statistics?.viewCount || 0), 0) / items.length);
        // Estimate posts/month from publish dates
        const dates = items.map(v => new Date(v.snippet.publishedAt)).sort((a,b) => b-a);
        if (dates.length >= 2) {
          const spanDays = (dates[0] - dates[dates.length-1]) / 86400000;
          postsPerMonth = spanDays > 0 ? Math.round((dates.length / spanDays) * 30 * 10) / 10 : null;
        }
      }
    }
  }

  return {
    channel_id:    item.id,
    name:          item.snippet.title,
    handle:        item.snippet.customUrl || null,
    avatar_url:    item.snippet.thumbnails?.high?.url || item.snippet.thumbnails?.default?.url || null,
    subscribers:   Number(item.statistics.subscriberCount || 0),
    total_views:   Number(item.statistics.viewCount || 0),
    video_count:   Number(item.statistics.videoCount || 0),
    avg_recent_views: avgViews,
    posts_per_month:  postsPerMonth,
    country:       item.snippet.country || null,
    banner_url:    item.brandingSettings?.image?.bannerExternalUrl || null,
  };
}

// Resolve channel URL/handle/ID → channel ID
async function resolveChannelId(input) {
  const apiKey = process.env.YOUTUBE_API_KEY;
  input = input.trim();

  // 1. Direct channel ID
  if (/^UC[A-Za-z0-9_-]{20,}$/.test(input)) return input;

  // 2. Extract identifier from URL
  let identifier = input;
  const urlPatterns = [
    /youtube\.com\/channel\/(UC[A-Za-z0-9_-]{20,})/,  // channel ID in URL → return directly
    /youtube\.com\/@([A-Za-z0-9_.\-]+)/,
    /youtube\.com\/c\/([A-Za-z0-9_.\-]+)/,
    /youtube\.com\/user\/([A-Za-z0-9_.\-]+)/,
  ];
  for (const p of urlPatterns) {
    const m = input.match(p);
    if (m) { identifier = m[1]; break; }
  }
  if (/^UC[A-Za-z0-9_-]{20,}$/.test(identifier)) return identifier;

  // Clean @ prefix
  const handle = identifier.startsWith('@') ? identifier.slice(1) : identifier;

  const ytGet = url => fetch(url).then(r => r.json()).catch(() => ({}));

  // 3. forHandle without @ (most reliable for new-style handles)
  let d = await ytGet(`https://www.googleapis.com/youtube/v3/channels?part=id&forHandle=${encodeURIComponent(handle)}&key=${apiKey}`);
  if (d.items?.[0]?.id) return d.items[0].id;

  // 4. forHandle with @ prefix (some channels need this)
  d = await ytGet(`https://www.googleapis.com/youtube/v3/channels?part=id&forHandle=${encodeURIComponent('@' + handle)}&key=${apiKey}`);
  if (d.items?.[0]?.id) return d.items[0].id;

  // 5. forUsername (legacy channels)
  d = await ytGet(`https://www.googleapis.com/youtube/v3/channels?part=id&forUsername=${encodeURIComponent(handle)}&key=${apiKey}`);
  if (d.items?.[0]?.id) return d.items[0].id;

  // 6. Search — try to find exact handle match first, then fall back to first result
  d = await ytGet(`https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q=${encodeURIComponent(handle)}&maxResults=5&key=${apiKey}`);
  const results = d.items || [];
  const exact = results.find(r =>
    r.snippet?.customUrl?.replace('@','').toLowerCase() === handle.toLowerCase()
  );
  const best = exact || results[0];
  if (best?.id?.channelId) return best.id.channelId;

  throw new Error(`Channel not found. Try pasting the full YouTube URL (e.g. youtube.com/@handle)`);
}

app.get('/api/competitors', async (req, res) => {
  try {
    const r = await pool.query(`SELECT * FROM competitors ORDER BY added_at ASC`);
    if (!r.rows.length) return res.json([]);

    // Batch-fetch live subscriber/view counts (cheap, 1 API call)
    const apiKey = process.env.YOUTUBE_API_KEY;
    const ids = r.rows.map(c => c.channel_id).join(',');
    const statsR = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=${ids}&key=${apiKey}`);
    const statsData = await statsR.json();
    const liveMap = {};
    (statsData.items || []).forEach(item => {
      liveMap[item.id] = {
        subscribers: Number(item.statistics.subscriberCount || 0),
        total_views: Number(item.statistics.viewCount || 0),
        video_count: Number(item.statistics.videoCount || 0),
        avatar_url:  item.snippet.thumbnails?.high?.url || item.snippet.thumbnails?.default?.url,
        name:        item.snippet.title,
      };
    });

    // Background-refresh avg_recent_views/posts_per_month for stale entries (>24h)
    const stale = r.rows.filter(c => !c.stats_updated_at || Date.now() - new Date(c.stats_updated_at) > 86400000);
    if (stale.length) {
      Promise.all(stale.map(async c => {
        try {
          const s = await fetchChannelStats(c.channel_id);
          await pool.query(
            `UPDATE competitors SET avg_recent_views=$1, posts_per_month=$2, stats_updated_at=NOW() WHERE id=$3`,
            [s.avg_recent_views, s.posts_per_month, c.id]
          );
        } catch(_) {}
      })).catch(() => {});
    }

    const result = r.rows.map(c => ({
      ...c,
      avg_recent_views: c.avg_recent_views ? Number(c.avg_recent_views) : null,
      posts_per_month:  c.posts_per_month  ? Number(c.posts_per_month)  : null,
      ...(liveMap[c.channel_id] || {}),
    }));
    res.json(result);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/competitors', express.json(), async (req, res) => {
  try {
    const { input } = req.body;
    if (!input) return res.status(400).json({ error: 'Missing channel URL or handle' });
    const channelId = await resolveChannelId(input);
    const stats     = await fetchChannelStats(channelId);
    const r = await pool.query(
      `INSERT INTO competitors (channel_id, name, handle, avatar_url, avg_recent_views, posts_per_month, stats_updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,NOW())
       ON CONFLICT (channel_id) DO UPDATE SET
         name=EXCLUDED.name, handle=EXCLUDED.handle, avatar_url=EXCLUDED.avatar_url,
         avg_recent_views=EXCLUDED.avg_recent_views, posts_per_month=EXCLUDED.posts_per_month,
         stats_updated_at=NOW()
       RETURNING *`,
      [channelId, stats.name, stats.handle, stats.avatar_url, stats.avg_recent_views, stats.posts_per_month]
    );
    res.json({ ...r.rows[0], ...stats });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/competitors/:id', async (req, res) => {
  try {
    await pool.query(`DELETE FROM competitors WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/my-channel', async (req, res) => {
  try {
    const channelId = (process.env.YOUTUBE_CHANNEL_ID || '').replace(/[^a-zA-Z0-9_-]/g, '');
    if (!channelId) return res.status(400).json({ error: 'No YouTube channel connected' });
    const stats = await fetchChannelStats(channelId);
    res.json(stats);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Recent videos for a competitor channel (last 12 videos with view counts)
app.get('/api/competitors/:channelId/videos', async (req, res) => {
  try {
    const apiKey = process.env.YOUTUBE_API_KEY;
    const { channelId } = req.params;
    if (!/^UC[A-Za-z0-9_-]{20,}$/.test(channelId)) return res.status(400).json({ error: 'Invalid channel ID' });

    // Get uploads playlist
    const chR = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=${channelId}&key=${apiKey}`);
    const chData = await chR.json();
    const uploadsId = chData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
    if (!uploadsId) return res.json([]);

    // Get last 12 video IDs
    const plR = await fetch(`https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=${uploadsId}&maxResults=12&key=${apiKey}`);
    const plData = await plR.json();
    const videoIds = (plData.items || []).map(i => i.contentDetails.videoId);
    if (!videoIds.length) return res.json([]);

    // Fetch stats + snippets
    const vR = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${videoIds.join(',')}&key=${apiKey}`);
    const vData = await vR.json();

    // Compute channel avg for outlier score
    const views = (vData.items || []).map(v => Number(v.statistics?.viewCount || 0));
    const avgViews = views.reduce((a,b) => a+b, 0) / (views.length || 1);

    const videos = (vData.items || []).map(v => {
      const views = Number(v.statistics?.viewCount || 0);
      const score = Math.min(100, Math.max(1, Math.round(views / (avgViews || 1) * 50)));
      return {
        id:            v.id,
        title:         v.snippet.title,
        thumbnail:     v.snippet.thumbnails?.medium?.url || v.snippet.thumbnails?.default?.url,
        published_at:  v.snippet.publishedAt,
        views,
        likes:         Number(v.statistics?.likeCount || 0),
        comments:      Number(v.statistics?.commentCount || 0),
        score,
      };
    });
    res.json(videos);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/competitors/details', async (req, res) => {
  try {
    const { ids } = req.query; // comma-separated channel IDs
    if (!ids) return res.json([]);
    const results = await Promise.all(ids.split(',').map(id => fetchChannelStats(id.trim()).catch(e => ({ channel_id: id, error: e.message }))));
    res.json(results);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── Outliers ──────────────────────────────────────────────────────────────
app.get('/api/outliers', async (req, res) => {
  try {
    const platform = req.query.platform || 'youtube';
    // Use median (PERCENTILE_CONT 0.5) as the baseline — less skewed by one-off viral hits
    const r = await pool.query(`
      WITH stats AS (
        SELECT
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY views) AS median_views,
          AVG(COALESCE(views,0))                             AS avg_views,
          COUNT(*)                                           AS total_videos
        FROM content_analytics
        WHERE platform = $1 AND views > 0
      )
      SELECT
        ca.post_id, ca.title, ca.thumbnail_url, ca.published_at,
        ca.views, ca.likes, ca.comments, ca.engagement_rate,
        ca.watch_time_minutes, ca.ctr, ca.avg_view_duration,
        s.median_views, s.avg_views, s.total_videos,
        ROUND((COALESCE(ca.views,0)::numeric / NULLIF(s.median_views,0)) * 10) / 10 AS view_ratio
      FROM content_analytics ca, stats s
      WHERE ca.platform = $1 AND ca.views > 0 AND ca.title IS NOT NULL
      ORDER BY view_ratio DESC NULLS LAST
      LIMIT 50
    `, [platform]);
    res.json(r.rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── YouTube-wide outlier search ────────────────────────────────────────────
const _ytOutlierCache = new Map();
const YT_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours — preserve daily quota

const DIVERSE_TOPICS = [
  'documentary', 'science experiment', 'history mystery',
  'true crime investigation', 'life advice wisdom', 'travel adventure',
  'technology explained', 'psychology human behavior', 'health transformation',
  'investing money finance', 'comedy skit', 'news analysis',
  'cooking technique', 'wildlife encounter', 'engineering build',
  'philosophy deep', 'economics explained', 'motivation success story',
  'art creation', 'education learning', 'unsolved mystery',
  'behind the scenes', 'exposing secrets', 'survival challenge',
  'social experiment', 'incredible story', 'amazing discovery',
];

function parseDurationSecs(str) {
  if (!str) return 0;
  const m = str.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return 0;
  return (parseInt(m[1]||0)*3600) + (parseInt(m[2]||0)*60) + (parseInt(m[3]||0));
}
function fmtDuration(s) {
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), ss = s%60;
  if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
  return `${m}:${String(ss).padStart(2,'0')}`;
}
function timeAgo(dateStr) {
  if (!dateStr) return '';
  const diff = Date.now() - new Date(dateStr).getTime();
  const d = Math.floor(diff/86400000);
  if (d < 1) return 'today';
  if (d < 7) return `${d}d ago`;
  if (d < 30) return `${Math.floor(d/7)}wk ago`;
  if (d < 365) return `${Math.floor(d/30)}mo ago`;
  return `${Math.floor(d/365)}yr ago`;
}

// Debug: test raw YouTube search
app.get('/api/debug/yt', async (req, res) => {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) return res.json({ error: 'No YOUTUBE_API_KEY', key: null });
  const q = req.query.q || 'documentary';
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&q=${encodeURIComponent(q)}&maxResults=3&key=${apiKey}`;
  const r = await fetch(url).then(r => r.json());
  res.json({ keyPrefix: apiKey.slice(0,10)+'…', url: url.replace(apiKey,'[KEY]'), response: r });
});

app.get('/api/outliers/youtube', async (req, res) => {
  const apiKey = process.env.YOUTUBE_API_KEY;
  if (!apiKey) return res.status(400).json({ error: 'YOUTUBE_API_KEY not set' });

  const q      = (req.query.q || '').trim();
  const order  = ['viewCount','relevance','date','rating'].includes(req.query.order) ? req.query.order : 'relevance';
  const publishedAfter = req.query.publishedAfter || '';
  const forceRefresh = req.query.refresh === '1';

  // Pick topics: specific query OR 2 from a time-based slot so the cache actually hits.
  // Slot advances every 4 hours → ~6 different pairs per day, each cached for 24hr.
  // Shuffle param shifts the slot by 1 so the button gives genuinely different results.
  const slotShift = parseInt(req.query.slot || '0');
  const slot = (Math.floor(Date.now() / (4 * 3600 * 1000)) + slotShift) % DIVERSE_TOPICS.length;
  const topics = q ? [q] : [
    DIVERSE_TOPICS[slot % DIVERSE_TOPICS.length],
    DIVERSE_TOPICS[(slot + 1) % DIVERSE_TOPICS.length],
  ];
  const cacheKey = topics.join('|') + ':' + order + ':' + publishedAfter;

  if (!forceRefresh) {
    const hit = _ytOutlierCache.get(cacheKey);
    if (hit && Date.now() - hit.ts < YT_CACHE_TTL) return res.json({ results: hit.data, cached: true });
  }

  try {
    // 1. Search each topic (100 quota units each)
    const allItems = [], allChannelIds = new Set();
    for (const term of topics) {
      let url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&q=${encodeURIComponent(term)}&order=${order}&maxResults=15&key=${apiKey}`;
      if (publishedAfter) url += `&publishedAfter=${encodeURIComponent(publishedAfter)}`;
      const r = await fetch(url).then(r => r.json());
      if (r.error) {
        const msg = r.error.message || JSON.stringify(r.error);
        if (r.error.errors?.[0]?.reason === 'quotaExceeded' || msg.toLowerCase().includes('quota')) {
          throw new Error('QUOTA_EXCEEDED');
        }
        throw new Error(`YouTube API error (search "${term}"): ${msg}`);
      }
      for (const item of (r.items || [])) {
        if (item.id?.videoId) { allItems.push(item); if (item.snippet?.channelId) allChannelIds.add(item.snippet.channelId); }
      }
    }
    if (!allItems.length) return res.json({ results: [], cached: false });

    // Cap at 50 — YouTube batch endpoints reject longer lists
    const videoIds   = [...new Set(allItems.map(i => i.id.videoId))].slice(0, 50);
    const channelIds = [...allChannelIds].slice(0, 50);

    // 2. Batch fetch video stats+contentDetails + channel stats
    const [videosD, channelsD] = await Promise.all([
      fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet,contentDetails&id=${videoIds.join(',')}&key=${apiKey}`).then(r => r.json()),
      fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id=${channelIds.join(',')}&key=${apiKey}`).then(r => r.json()),
    ]);
    if (videosD.error)   throw new Error(`YouTube videos API: ${videosD.error.message}`);
    if (channelsD.error) throw new Error(`YouTube channels API: ${channelsD.error.message}`);

    // 3. Build channel map
    const chanMap = {};
    for (const ch of (channelsD.items || [])) {
      const s = ch.statistics || {}, vc = parseInt(s.videoCount)||1, tv = parseInt(s.viewCount)||0;
      chanMap[ch.id] = {
        name: ch.snippet?.title || 'Unknown',
        handle: ch.snippet?.customUrl || '',
        avatar: ch.snippet?.thumbnails?.default?.url || null,
        subscribers: parseInt(s.subscriberCount)||0,
        avg_views: Math.round(tv / vc),
        video_count: vc,
      };
    }

    // 4. Build results — minimal filtering so we always get results
    const results = (videosD.items || []).map(v => {
      const stats = v.statistics || {};
      const views = parseInt(stats.viewCount)||0;
      const chanId = v.snippet?.channelId;
      const chan = chanMap[chanId] || {};
      const durationSecs = parseDurationSecs(v.contentDetails?.duration);
      const avgViews = chan.avg_views || 0;
      // Protect against avg=0 (hidden stats) — use views itself as baseline so ratio=1
      const ratio = avgViews > 0 ? Math.round((views / avgViews) * 10) / 10 : 1.0;
      return {
        video_id:          v.id,
        title:             v.snippet?.title || '',
        thumbnail_url:     v.snippet?.thumbnails?.maxres?.url || v.snippet?.thumbnails?.high?.url || null,
        published_at:      v.snippet?.publishedAt || null,
        duration_secs:     durationSecs,
        duration_fmt:      durationSecs >= 60 ? fmtDuration(durationSecs) : null,
        time_ago:          timeAgo(v.snippet?.publishedAt),
        views,
        channel_id:        chanId,
        channel_name:      chan.name,
        channel_handle:    chan.handle,
        channel_avatar:    chan.avatar,
        subscribers:       chan.subscribers,
        channel_avg_views: avgViews,
        video_count:       chan.video_count,
        view_ratio:        ratio,
      };
    })
    // Filter out Shorts
    .filter(r => r.duration_secs === 0 || r.duration_secs >= 120)
    // Exclude mega-popular channels (>5M subs) and mega-viral videos (>3M views)
    // We want mid-size creators having a breakout moment, not YouTube's biggest stars
    .filter(r => (r.subscribers === 0 || r.subscribers <= 5_000_000) && r.views <= 3_000_000)
    // Deduplicate — max 1 video per channel
    .reduce((acc, r) => {
      if (!acc.seen.has(r.channel_id)) { acc.seen.add(r.channel_id); acc.list.push(r); }
      return acc;
    }, { seen: new Set(), list: [] }).list
    .sort((a, b) => b.view_ratio - a.view_ratio);

    _ytOutlierCache.set(cacheKey, { ts: Date.now(), data: results });
    res.json({ results, cached: false });
  } catch(err) {
    if (err.message === 'QUOTA_EXCEEDED') {
      return res.status(429).json({ error: 'quota_exceeded', message: "YouTube API daily quota reached. Results reset at midnight Pacific time. Try again tomorrow, or hit Shuffle to view a different cached set." });
    }
    res.status(500).json({ error: err.message });
  }
});

// ── Lab: AI Title Suggestions from thumbnail ───────────────────────────────
app.post('/api/lab/suggest-titles', express.json({ limit: '5mb' }), async (req, res) => {
  try {
    if (!process.env.ANTHROPIC_API_KEY) return res.status(400).json({ error: 'ANTHROPIC_API_KEY not set' });
    const { image } = req.body; // base64 data URL
    if (!image) return res.status(400).json({ error: 'No image provided' });

    // Strip data URL prefix
    const matches = image.match(/^data:image\/(png|jpe?g|gif|webp);base64,(.+)$/);
    if (!matches) return res.status(400).json({ error: 'Invalid image format' });
    const mediaType = matches[1] === 'jpg' ? 'image/jpeg' : `image/${matches[1]}`;
    const b64 = matches[2];

    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    const resp = await client.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mediaType, data: b64 } },
          { type: 'text', text: `You are a YouTube title expert. Look at this thumbnail image and suggest 5 compelling YouTube video titles that would match it perfectly. The channel focuses on biblical history and ancient civilizations.

Return ONLY a JSON array of 5 title strings, nothing else. Example:
["Title one here","Title two here","Title three here","Title four here","Title five here"]` }
        ]
      }]
    });
    const text = resp.content[0]?.text?.trim() || '[]';
    const startIdx = text.indexOf('[');
    const endIdx   = text.lastIndexOf(']');
    const jsonStr  = startIdx >= 0 && endIdx > startIdx ? text.slice(startIdx, endIdx + 1) : '[]';
    res.json({ titles: JSON.parse(jsonStr) });
  } catch(err) { res.status(500).json({ error: err.message }); }
});



// ── Stripe ────────────────────────────────────────────────────────────────
app.post('/api/stripe/checkout', express.json(), async (req, res) => {
  try {
    if (!stripe) return res.status(400).json({ error: 'Stripe not configured — set STRIPE_SECRET_KEY in environment' });
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: process.env.STRIPE_PRICE_ID, quantity: 1 }],
      mode: 'subscription',
      success_url: `${req.headers.origin || 'http://localhost:3000'}/?upgraded=1`,
      cancel_url: `${req.headers.origin || 'http://localhost:3000'}/`,
    });
    res.json({ url: session.url });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  // webhook handling stub - implement after Stripe dashboard setup
  res.json({ received: true });
});

// One-time setup helper — retrieve stored YT refresh token from DB
app.get('/api/admin/setup', async (req, res) => {
  const secret = req.query.secret;
  if (!secret || secret !== process.env.ADMIN_SECRET) {
    return res.status(401).json({ error: 'Pass ?secret=YOUR_ADMIN_SECRET query param' });
  }
  try {
    const r = await pool.query('SELECT email, yt_refresh_token, yt_channel_id FROM users ORDER BY created_at LIMIT 1');
    if (!r.rows.length) return res.json({ message: 'No users in DB' });
    res.json({
      email: r.rows[0].email,
      yt_channel_id: r.rows[0].yt_channel_id,
      yt_refresh_token: r.rows[0].yt_refresh_token,
      next: 'Set GOOGLE_REFRESH_TOKEN = yt_refresh_token value in your Vercel env vars, then redeploy'
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Dashboard → http://localhost:${PORT}`));
