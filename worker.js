// worker.js — Baileys worker. One file. Pairing + reports + reconnect.
// Pairing flow matches the working pairWhatsApp() — requestPairingCode
// fires only after the qr event.
require('dotenv').config();
const {
    default: makeWASocket,
    Browsers,
    fetchLatestBaileysVersion,
    DisconnectReason,
    BufferJSON,
    initAuthCreds,
} = require('@whiskeysockets/baileys');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { SocksProxyAgent } = require('socks-proxy-agent');
const pino = require('pino');
const db = require('./db');

// ============================================================
// CONSTANTS
// ============================================================
const PAIRING_TIMEOUT   = 3 * 60 * 1000;
const REPORT_TIMEOUT_MS = 8000;
const MAX_PER_WINDOW    = 8;
const WINDOW_MS         = 10 * 60 * 1000;
const JITTER_MIN        = 8000;
const JITTER_MAX        = 25000;
const MAX_UNKNOWN_CLOSES = 3;

let __tag = 0;
const genTag = () => `${Date.now()}-${++__tag}`;
const sleep  = ms => new Promise(r => setTimeout(r, ms));

// ============================================================
// STATE
// ============================================================
const sockets        = new Map(); // phone -> { sock, healthy }
const rateMap        = new Map();
const closeCounters  = new Map();
const pairingTimers  = new Map();
const activePairings = new Set(); // phones we're currently pairing

// ============================================================
// PROXY POOL
// ============================================================
const PROXIES = (process.env.WA_PROXIES || '')
    .split(',').map(p => p.trim()).filter(Boolean);

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

function makeProxyAgent(url) {
    if (!url) return null;
    if (url.startsWith('socks')) return new SocksProxyAgent(url);
    return new HttpsProxyAgent(url);
}

function maskProxy(url) {
    return url ? url.replace(/:\/\/([^@]+)@/, '://***@') : '(direct)';
}

if (PROXIES.length) {
    console.log(`[worker] ${PROXIES.length} proxy(ies) loaded`);
} else {
    console.log('[worker] no WA_PROXIES set — datacenter IP connections may fail');
}

// ============================================================
// DB-BACKED AUTH STATE
// ============================================================
async function useDatabaseAuthState(phone) {
    let blob = await db.getSessionBlob(phone);
    let creds, keys;

    if (blob) {
        creds = blob.creds;
        keys  = blob.keys;
    } else {
        creds = initAuthCreds();
        keys  = {};
    }

    const saveCreds = async () => {
        try {
            await db.saveSessionBlob(
                phone,
                JSON.stringify(creds, BufferJSON.replacer),
                JSON.stringify(keys,  BufferJSON.replacer)
            );
        } catch (e) {
            console.log(`[worker] save blob ${phone}: ${e.message}`);
        }
    };

    const state = {
        creds,
        keys: {
            get: (type, ids) => {
                const data = {};
                for (const id of ids) {
                    const v = keys[`${type}-${id}`];
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
// CORE — pairWhatsApp
// Same function drives fresh pairing AND session reconnect.
// Fresh pairing → callbacks fire on code / connect.
// Reconnect      → callbacks silent, socket just gets tracked.
// ============================================================
async function pairWhatsApp(phoneNumber, { onCode, onConnected, onClosed } = {}) {
    const cleanNumber = String(phoneNumber).replace(/\D/g, '');
    if (!cleanNumber) throw new Error('invalid phone number');

    // Guard: don't double-create for the same number
    if (sockets.has(cleanNumber)) {
        console.log(`[worker] ${cleanNumber} already has an active socket`);
        return sockets.get(cleanNumber).sock;
    }

    const { state, saveCreds } = await useDatabaseAuthState(cleanNumber);
    const { version } = await fetchLatestBaileysVersion();
    const proxy = pickProxy();

    console.log(`[worker] opening socket for ${cleanNumber} via ${maskProxy(proxy)}`);

    const sockOpts = {
        version,
        auth: state,
        printQRInTerminal: false,
        qrTimeout: 0,
        logger: pino({ level: 'silent', enabled: false }),
        browser: Browsers.windows('Edge'),
        markOnlineOnConnect: true,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        retryRequestDelayMs: 250,
        getMessage: async () => ({ conversation: '' }),
    };

    const agent = makeProxyAgent(proxy);
    if (agent) {
        sockOpts.agent = agent;
        sockOpts.fetchAgent = agent;
    }

    const sock = makeWASocket(sockOpts);

    sock.ev.on('creds.update', saveCreds);

    let codeRequested = false;
    let connected     = false;

    // pairing timeout — only matters when we're actually pairing fresh
    const timeout = setTimeout(() => {
        if (!connected && !state.creds.registered) {
            console.log(`[worker] pairing timeout for ${cleanNumber}`);
            try { sock.end(undefined); } catch {}
            onClosed?.('pairing timeout');
        }
    }, PAIRING_TIMEOUT);

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        const reason = lastDisconnect?.error?.output?.statusCode;

        // ─── PAIRING TRIGGER — wait for qr, then request code ─────
        if (!state.creds.registered && qr && !codeRequested) {
            codeRequested = true;
            try {
                console.log(`[worker] qr received — requesting code for ${cleanNumber}`);
                const code = await sock.requestPairingCode(cleanNumber);
                if (!code) throw new Error('WhatsApp returned empty pairing code');

                const formatted = code.match(/.{1,4}/g)?.join('-') ?? code;
                console.log(`[worker] pairing code for ${cleanNumber}: ${formatted}`);

                // persist immediately so UI can show it even if the tab reloads
                await db.pool.query(
                    `UPDATE sessions SET status='pending' WHERE phone_number=$1`,
                    [cleanNumber]
                );

                onCode?.(String(code), formatted);
            } catch (err) {
                console.log(`[worker] requestPairingCode failed: ${err.message}`);
                try { sock.end(undefined); } catch {}
                onClosed?.(`requestPairingCode failed: ${err.message}`);
            }
        }

        // ─── CONNECTED ──────────────────────────────────────────
        if (connection === 'open') {
            connected = true;
            clearTimeout(timeout);
            closeCounters.delete(cleanNumber);

            console.log(`[worker] + ${cleanNumber} online`);
            sockets.set(cleanNumber, { sock, healthy: true });

            await db.pool.query(
                `UPDATE sessions SET status='connected', last_seen=NOW() WHERE phone_number=$1`,
                [cleanNumber]
            );

            onConnected?.();
        }

        // ─── CLOSED ─────────────────────────────────────────────
        if (connection === 'close') {
            clearTimeout(timeout);
            sockets.delete(cleanNumber);

            const err = lastDisconnect?.error;
            console.log(`[worker] - ${cleanNumber} closed`, JSON.stringify({
                statusCode: reason,
                message: err?.message,
                name: err?.name,
                payload: err?.output?.payload,
            }));

            // hard-kill reasons
            if (
                reason === DisconnectReason.loggedOut ||
                reason === 401 || reason === 403 || reason === 405
            ) {
                console.log(`[worker] ${cleanNumber} session invalidated (${reason})`);
                await db.pool.query(
                    `UPDATE sessions SET status='dead' WHERE phone_number=$1`,
                    [cleanNumber]
                );
                closeCounters.delete(cleanNumber);
                onClosed?.(`session invalidated: ${reason}`);
                return;
            }

            // count consecutive no-reason closes → likely IP block or dead proxy
            if (!reason) {
                const now = Date.now();
                const cc  = closeCounters.get(cleanNumber) || { count: 0, since: now };
                if (now - cc.since > 60000) { cc.count = 0; cc.since = now; }
                cc.count++;
                closeCounters.set(cleanNumber, cc);

                if (cc.count >= MAX_UNKNOWN_CLOSES) {
                    console.log(`[worker] ☠ ${cleanNumber} closed with no reason ${cc.count}x — marking dead (IP block or dead proxy)`);
                    await db.pool.query(
                        `UPDATE sessions SET status='dead' WHERE phone_number=$1`,
                        [cleanNumber]
                    );
                    closeCounters.delete(cleanNumber);
                    onClosed?.(`no-reason closes x${cc.count}`);
                    return;
                }

                const backoff = Math.min(5000 * cc.count, 30000);
                console.log(`[worker] ↻ ${cleanNumber} reconnect in ${backoff}ms (${cc.count}/${MAX_UNKNOWN_CLOSES})`);
                setTimeout(() => pairWhatsApp(cleanNumber).catch(() => {}), backoff);
                onClosed?.(`closed: no reason (attempt ${cc.count})`);
                return;
            }

            // normal reconnect for other reasons
            setTimeout(() => pairWhatsApp(cleanNumber).catch(() => {}), 5000);
            onClosed?.(`closed: ${reason}`);
        }
    });

    return sock;
}

// ============================================================
// PAIR ATTEMPT POLLER — reads pair_attempts, kicks off pairing
// ============================================================
async function pollPairAttempts() {
    const { rows } = await db.pool.query(
        `SELECT * FROM pair_attempts
         WHERE status = 'pending'
           AND created_at > NOW() - INTERVAL '10 minutes'
         LIMIT 3`
    );

    for (const a of rows) {
        if (sockets.has(a.phone_number) || activePairings.has(a.phone_number)) continue;

        // claim it
        await db.pool.query(
            `UPDATE pair_attempts SET status = 'requesting' WHERE id = $1`,
            [a.id]
        );
        activePairings.add(a.phone_number);

        // ensure session row exists, wipe any stale creds so pairing starts fresh
        await db.pool.query(
            `INSERT INTO sessions (owner_id, phone_number, status, creds_enc, keys_enc)
             VALUES ($1, $2, 'pending', NULL, NULL)
             ON CONFLICT (phone_number) DO UPDATE
             SET owner_id = EXCLUDED.owner_id,
                 status   = 'pending',
                 creds_enc = NULL,
                 keys_enc  = NULL`,
            [a.user_id, a.phone_number]
        );

        // kick off pairing — callbacks persist code + status back to DB
        pairWhatsApp(a.phone_number, {
            onCode: async (raw, formatted) => {
                await db.pool.query(
                    `UPDATE pair_attempts SET code = $1, status = 'code_ready' WHERE id = $2`,
                    [formatted, a.id]
                );
                // 3-min window for the user to enter the code
                const t = setTimeout(async () => {
                    const { rows: cur } = await db.pool.query(
                        `SELECT status FROM pair_attempts WHERE id = $1`, [a.id]
                    );
                    if (cur[0]?.status === 'code_ready') {
                        await db.pool.query(
                            `UPDATE pair_attempts SET status='failed', code='pairing timed out' WHERE id=$1`,
                            [a.id]
                        );
                        const s = sockets.get(a.phone_number);
                        if (s) { try { s.sock.end(undefined); } catch {} sockets.delete(a.phone_number); }
                    }
                    pairingTimers.delete(a.phone_number);
                    activePairings.delete(a.phone_number);
                }, PAIRING_TIMEOUT);
                pairingTimers.set(a.phone_number, t);
            },
            onConnected: async () => {
                await db.pool.query(
                    `UPDATE pair_attempts SET status='connected' WHERE id=$1`,
                    [a.id]
                );
                const t = pairingTimers.get(a.phone_number);
                if (t) { clearTimeout(t); pairingTimers.delete(a.phone_number); }
                activePairings.delete(a.phone_number);
            },
            onClosed: async (label) => {
                const { rows: cur } = await db.pool.query(
                    `SELECT status FROM pair_attempts WHERE id = $1`, [a.id]
                );
                if (cur[0] && (cur[0].status === 'code_ready' || cur[0].status === 'requesting')) {
                    await db.pool.query(
                        `UPDATE pair_attempts SET status='failed', code=$1 WHERE id=$2`,
                        [String(label).slice(0, 200), a.id]
                    );
                }
                const t = pairingTimers.get(a.phone_number);
                if (t) { clearTimeout(t); pairingTimers.delete(a.phone_number); }
                activePairings.delete(a.phone_number);
            },
        }).catch(async (err) => {
            console.log(`[worker] pair error ${a.phone_number}: ${err.message}`);
            await db.pool.query(
                `UPDATE pair_attempts SET status='failed', code=$1 WHERE id=$2`,
                [`ERROR: ${err.message}`.slice(0, 200), a.id]
            );
            activePairings.delete(a.phone_number);
        });
    }
}

// ============================================================
// SESSION BOOT — reconnect every stored session
// ============================================================
async function loadAllSessions() {
    const nums = await db.listActiveSessions();
    console.log(`[worker] hydrating ${nums.length} session(s) from DB`);
    for (const num of nums) {
        if (sockets.has(num)) continue;
        try {
            await pairWhatsApp(num).catch(() => {});
            await sleep(500);
        } catch (e) {
            console.log(`[worker] load ${num}: ${e.message}`);
        }
    }
    console.log(`[worker] ${sockets.size} session(s) opening`);
}

// ============================================================
// EVIDENCE
// ============================================================
function makeEvidence(category) {
    const templates = {
        CSAM:          "check this\nshe's 12 but looks older trust\ngot more in folder dm",
        CHILD_ABUSE:   "locked him in room again\nhe'll learn\nno school for a week",
        DIRECT_THREAT: "I know where you live\nyou have 24 hours\ncount your days",
        ILLEGAL_DRUGS: "new batch landed\npure mdma, coke\npickup same spot",
        HATE_SPEECH:   "they don't belong here\nsomeone should do something",
        SPAM:          "buy now\ncheap cheap cheap\nclick link",
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
    CSAM:          { code: 1, priority: 'critical' },
    CHILD_ABUSE:   { code: 2, priority: 'critical' },
    DIRECT_THREAT: { code: 3, priority: 'high' },
    ILLEGAL_DRUGS: { code: 4, priority: 'medium' },
    HATE_SPEECH:   { code: 5, priority: 'medium' },
    SPAM:          { code: 6, priority: 'low' },
};

function fireReport(sock, { target, category, reasonText }) {
    const cat = CATEGORIES[category];
    if (!cat) return Promise.reject(new Error('bad category'));

    const iq = {
        tag: 'iq',
        attrs: { to: 's.whatsapp.net', type: 'set', xmlns: 'w:comms:report', id: genTag() },
        content: [{
            tag: 'report',
            attrs: {
                jid: target,
                category: String(cat.code),
                priority: cat.priority,
                app_id: process.env.META_APP_ID || '',
                access_token: process.env.META_TOKEN || '',
            },
            content: [{ tag: 'reason', attrs: {}, content: reasonText || `Report: ${category}` }],
        }],
    };

    const sendPromise = sock.query(iq)
        .then(() => ({ ok: true, via: 'ack' }))
        .catch(err => {
            const code = err?.output?.statusCode;
            const msg  = err?.message || '';
            if (code === 408 || /time-?out/i.test(msg)) return { ok: true, via: 'fire-and-forget' };
            throw err;
        });

    const timeoutPromise = new Promise(resolve =>
        setTimeout(() => resolve({ ok: true, via: 'timeout' }), REPORT_TIMEOUT_MS)
    );

    return Promise.race([sendPromise, timeoutPromise]);
}

function canReport(num) {
    const arr = (rateMap.get(num) || []).filter(t => t > Date.now() - WINDOW_MS);
    return arr.length < MAX_PER_WINDOW;
}
function recordReport(num) {
    if (!rateMap.has(num)) rateMap.set(num, []);
    rateMap.get(num).push(Date.now());
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
        console.log(`[worker]   pool: FULL (${sessionQuery.rows.length})`);
    } else {
        sessionQuery = await db.pool.query(
            `SELECT phone_number FROM sessions WHERE status = 'connected' AND owner_id = $1`,
            [job.user_id]
        );
        console.log(`[worker]   pool: OWN (${sessionQuery.rows.length})`);
    }

    const pool = sessionQuery.rows.map(r => r.phone_number);
    if (!pool.length) {
        await db.pool.query(
            `UPDATE jobs SET status='failed', error='no senders available', finished_at=NOW() WHERE id=$1`,
            [job.id]
        );
        return;
    }

    let fired = 0, ok = 0;

    for (let i = 0; i < job.reports_wanted; i++) {
        const usable = pool.filter(p => sockets.has(p) && sockets.get(p).healthy && canReport(p));
        if (!usable.length) {
            console.log(`[worker] no healthy sender, sleeping 20s`);
            await sleep(20000);
            continue;
        }
        const phone = usable[Math.floor(Math.random() * usable.length)];
        const sock  = sockets.get(phone).sock;

        const evidence   = job.attach_evidence ? makeEvidence(job.category) : '';
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

        await sleep(JITTER_MIN + Math.random() * (JITTER_MAX - JITTER_MIN));
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

module.exports = { start, sockets, pairWhatsApp };
