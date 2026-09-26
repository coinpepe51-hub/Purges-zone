// worker.js — Baileys worker. Runs inside server.js process.
// Pairing logic mirrors the working pair.js — 3s delay before requestPairingCode.
require('dotenv').config();
const {
    default: makeWASocket,
    Browsers,
    fetchLatestBaileysVersion,
    DisconnectReason,
    BufferJSON,
    initAuthCreds,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const db = require('./db');

let __tag = 0;
const genTag = () => `${Date.now()}-${++__tag}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

const sockets = new Map();
const rateMap = new Map();

const MAX_PER_WINDOW = 8;
const WINDOW_MS = 10 * 60 * 1000;
const JITTER_MIN = 8000;
const JITTER_MAX = 25000;

const REPORT_TIMEOUT_MS = 8000;
const SOCKET_QUERY_TIMEOUT_MS = 15000;

function canReport(num) {
    const arr = (rateMap.get(num) || []).filter(t => t > Date.now() - WINDOW_MS);
    return arr.length < MAX_PER_WINDOW;
}
function recordReport(num) {
    if (!rateMap.has(num)) rateMap.set(num, []);
    rateMap.get(num).push(Date.now());
}

// ============================================================
// SOCKET OPTIONS — mirrors the working pair.js
// ============================================================
function socketOptions(authState, version) {
    return {
        logger: pino({ level: 'silent' }),
        printQRInTerminal: false,
        auth: authState,
        version,
        browser: Browsers.ubuntu('Edge'),
        getMessage: async () => ({ conversation: '' }),
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: SOCKET_QUERY_TIMEOUT_MS,
        keepAliveIntervalMs: 30000,
        emitOwnEvents: true,
        fireInitQueries: true,
        generateHighQualityLinkPreview: true,
        syncFullHistory: false,
        markOnlineOnConnect: false,
    };
}

// ============================================================
// AUTH STATE — hydrates from DB, saves back to DB
// ============================================================
async function useDatabaseAuthState(phone) {
    let blob = await db.getSessionBlob(phone);
    let creds, keys;

    if (blob) {
        creds = blob.creds;
        keys = blob.keys;
    } else {
        creds = initAuthCreds();
        keys = {};
    }

    const saveCreds = async () => {
        try {
            await db.saveSessionBlob(
                phone,
                JSON.stringify(creds, BufferJSON.replacer),
                JSON.stringify(keys, BufferJSON.replacer)
            );
        } catch (e) { console.log(`[worker] save blob ${phone}:`, e.message); }
    };

    const state = {
        creds,
        keys: {
            get: (type, ids) => {
                const data = {};
                for (const id of ids) {
                    let v = keys[`${type}-${id}`];
                    if (v) data[id] = v;
                }
                return data;
            },
            set: async (data) => {
                for (const cat of Object.keys(data)) {
                    for (const id of Object.keys(data[cat])) {
                        const v = data[cat][id];
                        if (v) keys[`${cat}-${id}`] = v;
                        else delete keys[`${cat}-${id}`];
                    }
                }
                await saveCreds();
            },
        },
    };

    return { state, saveCreds };
}

// ============================================================
// SESSION LOADING — for existing paired sessions
// ============================================================
async function loadSession(phone) {
    const { state, saveCreds } = await useDatabaseAuthState(phone);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket(socketOptions(state, version));

    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', async (u) => {
        const { connection, lastDisconnect } = u;
        const reason = lastDisconnect?.error?.output?.statusCode;

        if (connection === 'open') {
            console.log(`[worker] + ${phone} connected`);
            sockets.set(phone, { sock, healthy: true });
            await db.pool.query(
                `UPDATE sessions SET status='connected', last_seen=NOW() WHERE phone_number=$1`,
                [phone]
            );
        }
        if (connection === 'close') {
            console.log(`[worker] - ${phone} closed (${reason})`);
            const s = sockets.get(phone);
            if (s) s.healthy = false;
            if (reason === DisconnectReason.loggedOut || reason === 401 || reason === 403) {
                await db.pool.query(`UPDATE sessions SET status='dead' WHERE phone_number=$1`, [phone]);
                sockets.delete(phone);
            } else {
                setTimeout(() => loadSession(phone).catch(() => {}), 5000);
            }
        }
    });

    return sock;
}

async function loadAllSessions() {
    const nums = await db.listActiveSessions();
    console.log(`[worker] hydrating ${nums.length} session(s) from DB...`);
    for (const num of nums) {
        try { await loadSession(num); await sleep(300); } catch (e) {
            console.log(`[worker] load ${num}: ${e.message}`);
        }
    }
    console.log(`[worker] ${sockets.size} session(s) ready`);
}

// ============================================================
// PAIRING POLLER — mirrors pair.js flow
// ============================================================
async function pollPairAttempts() {
    const { rows } = await db.pool.query(
        `SELECT * FROM pair_attempts
         WHERE status = 'pending'
           AND created_at > NOW() - INTERVAL '10 minutes'
         LIMIT 3`
    );
    for (const a of rows) {
        if (sockets.has(a.phone_number)) continue;

        // mark claimed immediately so we don't double-process on the next poll
        await db.pool.query(
            `UPDATE pair_attempts SET status = 'requesting' WHERE id = $1`,
            [a.id]
        );

        try {
            // ensure the session row exists so blob writes have a target
            await db.pool.query(
                `INSERT INTO sessions (owner_id, phone_number, status)
                 VALUES ($1, $2, 'pending')
                 ON CONFLICT (phone_number) DO UPDATE
                 SET owner_id = EXCLUDED.owner_id, status = 'pending'`,
                [a.user_id, a.phone_number]
            );

            const { state, saveCreds } = await useDatabaseAuthState(a.phone_number);
            const { version } = await fetchLatestBaileysVersion();

            const sock = makeWASocket(socketOptions(state, version));
            sock.ev.on('creds.update', saveCreds);

            // ---- match pair.js: wait 3s before requesting code ----
            // the WS must be open for requestPairingCode to succeed
            await sleep(3000);

            // strip non-digits, same as pair.js
            let phoneNumber = a.phone_number.replace(/[^0-9]/g, '');
            if (!phoneNumber) throw new Error('invalid phone number after sanitize');

            let code;
            try {
                code = await sock.requestPairingCode(phoneNumber);
            } catch (err) {
                console.log(`[worker] requestPairingCode failed for ${a.phone_number}: ${err.message}`);
                await db.pool.query(
                    `UPDATE pair_attempts SET status = 'failed', code = $1 WHERE id = $2`,
                    [`ERROR: ${err.message}`.slice(0, 200), a.id]
                );
                try { sock.end(undefined); } catch {}
                continue;
            }

            if (!code) throw new Error('empty pairing code');
            code = code.match(/.{1,4}/g)?.join('-') || code;

            await db.pool.query(
                `UPDATE pair_attempts SET code = $1, status = 'code_ready' WHERE id = $2`,
                [code, a.id]
            );
            console.log(`[worker] pairing code for ${a.phone_number}: ${code}`);

            // wait for user to enter code
            sock.ev.on('connection.update', async (u) => {
                const { connection, lastDisconnect } = u;
                const reason = lastDisconnect?.error?.output?.statusCode;

                if (connection === 'open') {
                    await db.pool.query(
                        `UPDATE pair_attempts SET status='connected' WHERE id=$1`,
                        [a.id]
                    );
                    await db.pool.query(
                        `UPDATE sessions SET status='connected', last_seen=NOW() WHERE phone_number=$1`,
                        [a.phone_number]
                    );
                    sockets.set(a.phone_number, { sock, healthy: true });
                    console.log(`[worker] paired ${a.phone_number}`);
                }
                if (connection === 'close') {
                    console.log(`[worker] pair socket closed for ${a.phone_number} (${reason})`);
                    // only mark failed if we never got to connected
                    const { rows: cur } = await db.pool.query(
                        `SELECT status FROM pair_attempts WHERE id = $1`,
                        [a.id]
                    );
                    if (cur[0] && cur[0].status === 'code_ready') {
                        await db.pool.query(
                            `UPDATE pair_attempts SET status='failed' WHERE id=$1`,
                            [a.id]
                        );
                    }
                }
            });

            // safety timeout — if user never completes pairing within 10 min, mark failed
            setTimeout(async () => {
                const { rows: cur } = await db.pool.query(
                    `SELECT status FROM pair_attempts WHERE id = $1`,
                    [a.id]
                );
                if (cur[0] && (cur[0].status === 'code_ready' || cur[0].status === 'requesting')) {
                    await db.pool.query(
                        `UPDATE pair_attempts SET status='failed' WHERE id=$1`,
                        [a.id]
                    );
                    try { sock.end(undefined); } catch {}
                }
            }, 10 * 60 * 1000);

        } catch (e) {
            console.log(`[worker] pair fail ${a.phone_number}: ${e.message}`);
            await db.pool.query(
                `UPDATE pair_attempts SET status='failed', code = $1 WHERE id=$2`,
                [`ERROR: ${e.message}`.slice(0, 200), a.id]
            );
        }
    }
}

// ============================================================
// EVIDENCE
// ============================================================
function makeEvidence(category) {
    const templates = {
        CSAM: "check this\nshe's 12 but looks older trust\ngot more in folder dm",
        CHILD_ABUSE: "locked him in room again\nhe'll learn\nno school for a week",
        DIRECT_THREAT: "I know where you live\nyou have 24 hours\ncount your days",
        ILLEGAL_DRUGS: "new batch landed\npure mdma, coke\npickup same spot",
        HATE_SPEECH: "they don't belong here\nsomeone should do something",
        SPAM: "buy now\ncheap cheap cheap\nclick link",
    };
    const lines = (templates[category] || templates.SPAM).split('\n');
    return lines.map((l, i) =>
        `[${String(10 + i).padStart(2, '0')}:${String((i * 7) % 60).padStart(2, '0')}] ${i % 2 ? 'You' : 'Target'}: ${l}`
    ).join('\n');
}

// ============================================================
// REPORT SENDING — fire and forget
// ============================================================
const CATEGORIES = {
    CSAM: { code: 1, priority: 'critical' },
    CHILD_ABUSE: { code: 2, priority: 'critical' },
    DIRECT_THREAT: { code: 3, priority: 'high' },
    ILLEGAL_DRUGS: { code: 4, priority: 'medium' },
    HATE_SPEECH: { code: 5, priority: 'medium' },
    SPAM: { code: 6, priority: 'low' },
};

function fireReport(sock, { target, category, reasonText }) {
    const cat = CATEGORIES[category];
    if (!cat) return Promise.reject(new Error('bad category'));

    const reportNode = {
        tag: 'report',
        attrs: {
            jid: target,
            category: String(cat.code),
            priority: cat.priority,
            app_id: process.env.META_APP_ID || '',
            access_token: process.env.META_TOKEN || '',
        },
        content: [{ tag: 'reason', attrs: {}, content: reasonText || `Report: ${category}` }],
    };

    const iq = {
        tag: 'iq',
        attrs: { to: 's.whatsapp.net', type: 'set', xmlns: 'w:comms:report', id: genTag() },
        content: [reportNode],
    };

    const sendPromise = sock.query(iq)
        .then(() => ({ ok: true, via: 'ack' }))
        .catch(err => {
            const code = err?.output?.statusCode;
            const msg = err?.message || '';
            if (code === 408 || /time-?out/i.test(msg)) {
                return { ok: true, via: 'fire-and-forget' };
            }
            throw err;
        });

    const timeoutPromise = new Promise(resolve =>
        setTimeout(() => resolve({ ok: true, via: 'timeout' }), REPORT_TIMEOUT_MS)
    );

    return Promise.race([sendPromise, timeoutPromise]);
}

// ============================================================
// JOB RUNNER
// ============================================================
async function runJob(job) {
    console.log(`[worker] > job ${job.id} -> ${job.target} [${job.category}]`);

    const owner = await db.pool.query('SELECT tier, premium_until FROM users WHERE id = $1', [job.user_id]);
    const ownerRow = owner.rows[0];
    let tier = ownerRow?.tier || 'free';

    if (tier === 'premium' && ownerRow?.premium_until && new Date(ownerRow.premium_until) < new Date()) {
        tier = 'free';
    }

    const isPremium = tier === 'premium' || tier === 'admin';

    let sessionQuery;
    if (isPremium) {
        sessionQuery = await db.pool.query(
            `SELECT phone_number FROM sessions WHERE status = 'connected'`
        );
        console.log(`[worker]   pool: FULL (${sessionQuery.rows.length} node(s))`);
    } else {
        sessionQuery = await db.pool.query(
            `SELECT phone_number FROM sessions WHERE status = 'connected' AND owner_id = $1`,
            [job.user_id]
        );
        console.log(`[worker]   pool: OWN (${sessionQuery.rows.length} node(s))`);
    }

    const pool = sessionQuery.rows.map(r => r.phone_number);
    if (!pool.length) {
        await db.pool.query(
            `UPDATE jobs SET status='failed', error='no senders available', finished_at=NOW() WHERE id=$1`,
            [job.id]
        );
        return;
    }

    let fired = 0;
    let ok = 0;

    for (let i = 0; i < job.reports_wanted; i++) {
        const usable = pool.filter(p => sockets.has(p) && sockets.get(p).healthy && canReport(p));
        if (!usable.length) {
            console.log(`[worker] no healthy sender, sleeping 20s`);
            await sleep(20000);
            continue;
        }
        const phone = usable[Math.floor(Math.random() * usable.length)];
        const sock = sockets.get(phone).sock;

        const evidence = job.attach_evidence ? makeEvidence(job.category) : '';
        const reasonText = job.reason ? `${job.reason}\n\n---\n${evidence}` : evidence;

        try {
            const r = await fireReport(sock, { target: job.target, category: job.category, reasonText });
            fired++; ok++;
            recordReport(phone);
            await db.pool.query(
                `UPDATE jobs SET reports_fired=$1, reports_ok=$2 WHERE id=$3`,
                [fired, ok, job.id]
            );
            await db.pool.query(
                `UPDATE sessions SET reports_count = reports_count + 1, last_seen=NOW() WHERE phone_number=$1`,
                [phone]
            );
            console.log(`[worker]   + ${phone} [${i + 1}/${job.reports_wanted}] (${r.via})`);
        } catch (e) {
            fired++;
            console.log(`[worker]   - ${phone}: ${e.message}`);
            if (/401|403|logged out/i.test(e.message)) {
                const s = sockets.get(phone);
                if (s) s.healthy = false;
            }
        }

        const jitter = JITTER_MIN + Math.random() * (JITTER_MAX - JITTER_MIN);
        await sleep(jitter);
    }

    await db.pool.query(
        `UPDATE jobs SET status=$1, finished_at=NOW() WHERE id=$2`,
        [ok > 0 ? 'done' : 'failed', job.id]
    );
    console.log(`[worker] = job ${job.id} done - ${fired} fired, ${ok} ok`);
}

// ============================================================
// MAIN LOOP
// ============================================================
let running = false;

async function start() {
    if (running) return;
    running = true;

    await loadAllSessions();

    console.log('[worker] loop started');
    while (true) {
        try {
            await pollPairAttempts();

            const { rows } = await db.pool.query(
                `UPDATE jobs SET status='running', started_at=NOW()
                 WHERE id = (
                     SELECT id FROM jobs
                     WHERE status='pending'
                     ORDER BY priority DESC, created_at ASC
                     LIMIT 1
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING *`
            );
            if (!rows[0]) { await sleep(5000); continue; }
            await runJob(rows[0]);
        } catch (e) {
            console.error('[worker] loop error:', e.message);
            await sleep(5000);
        }
    }
}

module.exports = { start, sockets };
