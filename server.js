// server.js — Express API + frontend + worker + keep-alive
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const db = require('./db');

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const JWT_SECRET = process.env.JWT_SECRET || 'change-me';

// ============================================================
// TIER CONFIG
// ============================================================
const TIERS = {
    free: {
        maxNodes:      10,
        dailyReports:  5,
        perJob:        3,
        categories:    ['SPAM', 'HATE_SPEECH'],
        evidence:      false,
        priority:      0,
        fullPool:      false,
    },
    premium: {
        maxNodes:      50,
        dailyReports:  100,
        perJob:        20,
        categories:    ['CSAM', 'CHILD_ABUSE', 'DIRECT_THREAT', 'ILLEGAL_DRUGS', 'HATE_SPEECH', 'SPAM'],
        evidence:      true,
        priority:      10,
        fullPool:      true,
    },
    admin: {
        maxNodes:      999,
        dailyReports:  9999,
        perJob:        50,
        categories:    ['CSAM', 'CHILD_ABUSE', 'DIRECT_THREAT', 'ILLEGAL_DRUGS', 'HATE_SPEECH', 'SPAM'],
        evidence:      true,
        priority:      100,
        fullPool:      true,
    },
};

function tierOf(user) {
    if (user.tier === 'premium' && user.premium_until && new Date(user.premium_until) < new Date()) {
        return 'free';
    }
    return user.tier;
}

function configFor(user) {
    return TIERS[tierOf(user)] || TIERS.free;
}

function sanitizeJid(input) {
    const raw = String(input || '').trim();
    if (raw.includes('@')) return raw;
    const d = raw.replace(/\D/g, '');
    return d ? `${d}@s.whatsapp.net` : null;
}

function signToken(user) {
    return jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, { expiresIn: '30d' });
}

async function auth(req, res, next) {
    try {
        const h = req.headers.authorization || '';
        const token = h.startsWith('Bearer ') ? h.slice(7) : null;
        if (!token) return res.status(401).json({ error: 'no token' });
        const payload = jwt.verify(token, JWT_SECRET);
        const { rows } = await db.pool.query('SELECT * FROM users WHERE id = $1', [payload.id]);
        if (!rows[0] || rows[0].banned) return res.status(401).json({ error: 'invalid user' });
        req.user = rows[0];
        req.tier = tierOf(req.user);
        next();
    } catch { res.status(401).json({ error: 'bad token' }); }
}

function requireAdmin(req, res, next) {
    if (tierOf(req.user) !== 'admin') return res.status(403).json({ error: 'admin only' });
    next();
}

// ============================================================
// AUTH
// ============================================================
app.post('/api/auth/register', async (req, res) => {
    const { email, password } = req.body || {};
    if (!email || !password || password.length < 6) return res.status(400).json({ error: 'invalid input' });
    try {
        const hash = bcrypt.hashSync(password, 10);
        const { rows } = await db.pool.query(
            `INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING *`,
            [email.toLowerCase(), hash]
        );
        const user = rows[0];
        res.json({ token: signToken(user), user: { id: user.id, email: user.email, tier: tierOf(user), premium_until: user.premium_until } });
    } catch (e) {
        if (e.code === '23505') return res.status(400).json({ error: 'email taken' });
        res.status(500).json({ error: 'server error' });
    }
});

app.post('/api/auth/login', async (req, res) => {
    const { email, password } = req.body || {};
    const { rows } = await db.pool.query('SELECT * FROM users WHERE email = $1', [String(email).toLowerCase()]);
    const user = rows[0];
    if (!user || !bcrypt.compareSync(password, user.password_hash)) return res.status(401).json({ error: 'wrong credentials' });
    if (user.banned) return res.status(403).json({ error: 'banned' });

    const effectiveTier = tierOf(user);
    if (effectiveTier !== user.tier) {
        await db.pool.query('UPDATE users SET tier = $1 WHERE id = $2', [effectiveTier, user.id]);
        user.tier = effectiveTier;
    }

    res.json({ token: signToken(user), user: { id: user.id, email: user.email, tier: tierOf(user), premium_until: user.premium_until } });
});

app.get('/api/auth/me', auth, async (req, res) => {
    const cfg = configFor(req.user);
    res.json({
        user: {
            id: req.user.id,
            email: req.user.email,
            tier: req.tier,
            premium_until: req.user.premium_until,
        },
        limits: {
            maxNodes: cfg.maxNodes,
            dailyReports: cfg.dailyReports,
            perJob: cfg.perJob,
            categories: cfg.categories,
            evidence: cfg.evidence,
            fullPool: cfg.fullPool,
        },
    });
});

// ============================================================
// PAIRING
// ============================================================
app.post('/api/pair/start', auth, async (req, res) => {
    const { phoneNumber } = req.body || {};
    const clean = String(phoneNumber || '').replace(/\D/g, '');
    if (clean.length < 10 || clean.length > 15) return res.status(400).json({ error: 'bad number' });

    const cfg = configFor(req.user);
    const { rows: own } = await db.pool.query(
        'SELECT COUNT(*)::int as n FROM sessions WHERE owner_id = $1 AND status != $2',
        [req.user.id, 'dead']
    );
    if (own[0].n >= cfg.maxNodes) {
        return res.status(400).json({ error: `${req.tier} tier max ${cfg.maxNodes} nodes` });
    }

    const id = uuidv4();
    await db.pool.query(
        `INSERT INTO pair_attempts (id, phone_number, user_id) VALUES ($1, $2, $3)`,
        [id, clean, req.user.id]
    );
    res.json({ attemptId: id });
});

app.get('/api/pair/status/:id', auth, async (req, res) => {
    const { rows } = await db.pool.query(
        'SELECT * FROM pair_attempts WHERE id = $1 AND user_id = $2',
        [req.params.id, req.user.id]
    );
    if (!rows[0]) return res.status(404).json({ error: 'not found' });
    res.json({ status: rows[0].status, code: rows[0].code });
});

// ============================================================
// SESSIONS
// ============================================================
app.get('/api/sessions', auth, async (req, res) => {
    const { rows } = await db.pool.query(
        'SELECT id, phone_number, status, reports_count, last_seen FROM sessions WHERE owner_id = $1 ORDER BY created_at DESC',
        [req.user.id]
    );
    res.json({ sessions: rows });
});

app.delete('/api/sessions/:id', auth, async (req, res) => {
    await db.pool.query('DELETE FROM sessions WHERE id = $1 AND owner_id = $2', [req.params.id, req.user.id]);
    res.json({ ok: true });
});

// ============================================================
// REPORT
// ============================================================
app.post('/api/report', auth, async (req, res) => {
    const { target, category, reason, reports, attachEvidence } = req.body || {};
    const cfg = configFor(req.user);

    const targetJid = sanitizeJid(target);
    if (!targetJid) return res.status(400).json({ error: 'bad target' });

    if (!cfg.categories.includes(category)) return res.status(400).json({ error: 'category not in your tier' });

    let user = req.user;
    const today = new Date().toISOString().slice(0, 10);
    if (user.reports_reset.toISOString().slice(0, 10) !== today) {
        await db.pool.query('UPDATE users SET reports_today = 0, reports_reset = $1 WHERE id = $2', [today, user.id]);
        user.reports_today = 0;
    }

    const want = Math.min(parseInt(reports || String(cfg.perJob), 10), cfg.perJob);
    if (user.reports_today + want > cfg.dailyReports) {
        return res.status(429).json({ error: `daily cap ${cfg.dailyReports} reached` });
    }

    const id = uuidv4();
    await db.pool.query(
        `INSERT INTO jobs (id, user_id, target, category, reason, mode, reports_wanted, attach_evidence, priority)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [id, user.id, targetJid, category, reason || '', 'single', want, !!attachEvidence && cfg.evidence, cfg.priority]
    );
    await db.pool.query('UPDATE users SET reports_today = reports_today + $1 WHERE id = $2', [want, user.id]);

    res.json({ jobId: id });
});

app.get('/api/job/:id', auth, async (req, res) => {
    const { rows } = await db.pool.query(
        'SELECT * FROM jobs WHERE id = $1 AND (user_id = $2 OR $3 = true)',
        [req.params.id, req.user.id, req.tier === 'admin']
    );
    if (!rows[0]) return res.status(404).json({ error: 'not found' });
    const j = rows[0];
    res.json({
        id: j.id, status: j.status, target: j.target, category: j.category,
        fired: j.reports_fired, ok: j.reports_ok, error: j.error,
        createdAt: j.created_at, finishedAt: j.finished_at,
    });
});

app.get('/api/jobs', auth, async (req, res) => {
    const { rows } = await db.pool.query(
        'SELECT * FROM jobs WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50',
        [req.user.id]
    );
    res.json({ jobs: rows });
});

// ============================================================
// ADMIN
// ============================================================
app.get('/api/admin/stats', auth, requireAdmin, async (req, res) => {
    const users = await db.pool.query(`SELECT COUNT(*)::int n, COUNT(*) FILTER (WHERE tier='premium')::int p FROM users`);
    const sessions = await db.pool.query(`SELECT COUNT(*)::int n, COUNT(*) FILTER (WHERE status='connected')::int c FROM sessions`);
    const jobs = await db.pool.query(`SELECT COUNT(*)::int n, COUNT(*) FILTER (WHERE status='pending')::int p FROM jobs`);
    res.json({ users: users.rows[0], sessions: sessions.rows[0], jobs: jobs.rows[0] });
});

app.get('/api/admin/users', auth, requireAdmin, async (req, res) => {
    const { rows } = await db.pool.query(
        'SELECT id, email, tier, premium_until, banned, reports_today, created_at FROM users ORDER BY created_at DESC LIMIT 200'
    );
    res.json({ users: rows });
});

app.post('/api/admin/users/create', auth, requireAdmin, async (req, res) => {
    const { email, password, tier, durationDays } = req.body || {};
    if (!email || !password || password.length < 6) return res.status(400).json({ error: 'invalid email/password' });
    if (!['free', 'premium'].includes(tier)) return res.status(400).json({ error: 'bad tier' });

    try {
        const hash = bcrypt.hashSync(password, 10);
        let premiumUntil = null;
        if (tier === 'premium' && durationDays) {
            premiumUntil = new Date(Date.now() + parseInt(durationDays, 10) * 86400000);
        }

        const { rows } = await db.pool.query(
            `INSERT INTO users (email, password_hash, tier, premium_until)
             VALUES ($1, $2, $3, $4)
             RETURNING id, email, tier, premium_until, created_at`,
            [email.toLowerCase(), hash, tier, premiumUntil]
        );
        res.json({ user: rows[0] });
    } catch (e) {
        if (e.code === '23505') return res.status(400).json({ error: 'email taken' });
        res.status(500).json({ error: 'server error' });
    }
});

app.post('/api/admin/users/:id/tier', auth, requireAdmin, async (req, res) => {
    const { tier, durationDays } = req.body || {};
    if (!['free', 'premium', 'admin'].includes(tier)) return res.status(400).json({ error: 'bad tier' });

    let premiumUntil = null;
    if (tier === 'premium' && durationDays) {
        premiumUntil = new Date(Date.now() + parseInt(durationDays, 10) * 86400000);
    }

    await db.pool.query(
        'UPDATE users SET tier = $1, premium_until = $2 WHERE id = $3',
        [tier, premiumUntil, req.params.id]
    );
    res.json({ ok: true });
});

app.post('/api/admin/users/:id/ban', auth, requireAdmin, async (req, res) => {
    const { banned } = req.body || {};
    await db.pool.query('UPDATE users SET banned = $1 WHERE id = $2', [!!banned, req.params.id]);
    res.json({ ok: true });
});

app.delete('/api/admin/users/:id', auth, requireAdmin, async (req, res) => {
    await db.pool.query('DELETE FROM users WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
});

app.get('/api/admin/sessions', auth, requireAdmin, async (req, res) => {
    const { rows } = await db.pool.query(
        `SELECT s.*, u.email AS owner_email FROM sessions s
         LEFT JOIN users u ON u.id = s.owner_id
         ORDER BY s.created_at DESC LIMIT 500`
    );
    res.json({ sessions: rows });
});

app.delete('/api/admin/sessions/:id', auth, requireAdmin, async (req, res) => {
    await db.pool.query('DELETE FROM sessions WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
});

app.get('/api/admin/jobs', auth, requireAdmin, async (req, res) => {
    const { rows } = await db.pool.query(
        `SELECT j.*, u.email AS user_email FROM jobs j
         LEFT JOIN users u ON u.id = j.user_id
         ORDER BY j.created_at DESC LIMIT 200`
    );
    res.json({ jobs: rows });
});

// ---------- health ----------
app.get('/healthz', (_, res) => res.json({ ok: true, t: Date.now() }));

// SPA fallback
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ============================================================
// KEEP-ALIVE
// ============================================================
function startKeepAlive() {
    const base = process.env.RENDER_EXTERNAL_URL || process.env.PUBLIC_URL || null;
    if (!base) { console.log('[keepalive] no public URL — skipping'); return; }
    const url = base.replace(/\/$/, '') + '/healthz';
    const INTERVAL = 6 * 60 * 1000;

    const ping = async () => {
        try {
            const res = await fetch(url);
            console.log(`[keepalive] ${url} -> ${res.status}`);
        } catch (e) { console.log(`[keepalive] failed: ${e.message}`); }
    };

    ping();
    setInterval(ping, INTERVAL);
    console.log(`[keepalive] pinging ${url} every 6 min`);
}

const PORT = process.env.PORT || 3000;

db.init()
    .then(async () => {
        app.listen(PORT, () => console.log(`[server] :${PORT}`));

        if (process.env.WORKER_ENABLED !== 'false') {
            const worker = require('./worker');
            worker.start().catch(e => console.error('[worker] boot failed:', e));
        }

        startKeepAlive();
    })
    .catch(e => {
        console.error('DB init failed:', e);
        process.exit(1);
    });
