// db.js — Postgres helpers + encrypted session storage
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('sslmode=require') ? { rejectUnauthorized: false } : false,
});

const KEY = crypto.createHash('sha256').update(process.env.SESSION_KEY || 'purgers-default-key').digest();

function encrypt(text) {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', KEY, iv);
    const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    return Buffer.concat([iv, tag, enc]).toString('base64');
}

function decrypt(b64) {
    const buf = Buffer.from(b64, 'base64');
    const iv = buf.slice(0, 12);
    const tag = buf.slice(12, 28);
    const enc = buf.slice(28);
    const decipher = crypto.createDecipheriv('aes-256-gcm', KEY, iv);
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(enc), decipher.final()]).toString('utf8');
}

async function init() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            tier TEXT NOT NULL DEFAULT 'free',
            premium_until TIMESTAMPTZ,
            reports_today INT NOT NULL DEFAULT 0,
            reports_reset DATE NOT NULL DEFAULT CURRENT_DATE,
            banned BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id UUID REFERENCES users(id) ON DELETE CASCADE,
            phone_number TEXT UNIQUE NOT NULL,
            creds_enc TEXT,
            keys_enc TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            reports_count INT NOT NULL DEFAULT 0,
            last_seen TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            target TEXT NOT NULL,
            category TEXT NOT NULL,
            reason TEXT,
            mode TEXT NOT NULL DEFAULT 'single',
            reports_wanted INT NOT NULL DEFAULT 3,
            attach_evidence BOOLEAN NOT NULL DEFAULT FALSE,
            priority INT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            reports_fired INT NOT NULL DEFAULT 0,
            reports_ok INT NOT NULL DEFAULT 0,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(status, priority DESC, created_at ASC);
        CREATE TABLE IF NOT EXISTS pair_attempts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            phone_number TEXT NOT NULL,
            code TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    `);

    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMPTZ`);
    await pool.query(`ALTER TABLE sessions DROP COLUMN IF EXISTS shared_pool`);

    const adminEmail = process.env.ADMIN_EMAIL;
    const adminPass = process.env.ADMIN_PASSWORD;
    if (adminEmail && adminPass) {
        const bcrypt = require('bcryptjs');
        const hash = bcrypt.hashSync(adminPass, 10);
        await pool.query(
            `INSERT INTO users (email, password_hash, tier)
             VALUES ($1, $2, 'admin')
             ON CONFLICT (email) DO NOTHING`,
            [adminEmail, hash]
        );
    }
}

async function saveSessionBlob(phone, credsJson, keysJson) {
    await pool.query(
        `UPDATE sessions SET creds_enc = $1, keys_enc = $2 WHERE phone_number = $3`,
        [encrypt(credsJson), encrypt(keysJson), phone]
    );
}

async function getSessionBlob(phone) {
    const { rows } = await pool.query(
        `SELECT creds_enc, keys_enc FROM sessions WHERE phone_number = $1`,
        [phone]
    );
    if (!rows[0]?.creds_enc) return null;
    return {
        creds: JSON.parse(decrypt(rows[0].creds_enc)),
        keys: JSON.parse(decrypt(rows[0].keys_enc)),
    };
}

async function listActiveSessions() {
    const { rows } = await pool.query(
        `SELECT phone_number FROM sessions WHERE status != 'dead'`
    );
    return rows.map(r => r.phone_number);
}

module.exports = {
    pool, init,
    saveSessionBlob, getSessionBlob, listActiveSessions,
    encrypt, decrypt,
};
