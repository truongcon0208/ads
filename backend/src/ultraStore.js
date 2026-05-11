import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';

const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.resolve('data');
const STATE_FILE = path.join(DATA_DIR, 'ultra-job-state.json');
const DATABASE_URL = process.env.DATABASE_URL || '';

let stateCache = emptyState();
let dbPool = null;
let dbReady = false;
let dbError = null;
let writeQueue = Promise.resolve();

function emptyState() {
  return {
    version: 2,
    storage: DATABASE_URL ? 'postgres' : 'file',
    jobs: {},
    resume: {},
    updatedAt: new Date().toISOString()
  };
}

function ensureFileStore() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(STATE_FILE)) {
    fs.writeFileSync(STATE_FILE, JSON.stringify(emptyState(), null, 2), 'utf8');
  }
}

function readFileState() {
  ensureFileStore();
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return normalizeState(parsed);
  } catch {
    return emptyState();
  }
}

function writeFileState(nextState) {
  ensureFileStore();
  const next = normalizeState(nextState);
  const tmp = `${STATE_FILE}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(next, null, 2), 'utf8');
  fs.renameSync(tmp, STATE_FILE);
  return next;
}

function normalizeState(input = {}) {
  return {
    ...emptyState(),
    ...(input && typeof input === 'object' ? input : {}),
    jobs: input?.jobs && typeof input.jobs === 'object' ? input.jobs : {},
    resume: input?.resume && typeof input.resume === 'object' ? input.resume : {},
    updatedAt: input?.updatedAt || new Date().toISOString(),
    storage: dbReady ? 'postgres' : (DATABASE_URL ? 'postgres_pending' : 'file')
  };
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function shouldUseSslForDatabase(url) {
  if (!url) return false;
  if (process.env.DATABASE_SSL === 'true') return true;
  if (process.env.DATABASE_SSL === 'false') return false;
  return !url.includes('railway.internal') && !url.includes('localhost') && !url.includes('127.0.0.1');
}

async function ensureDbSchema() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS ultra_jobs (
      id TEXT PRIMARY KEY,
      kind TEXT,
      status TEXT,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS campaign_resume (
      key TEXT PRIMARY KEY,
      ad_account_id TEXT NOT NULL,
      page_id TEXT NOT NULL,
      campaign_id TEXT,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await dbPool.query(`CREATE INDEX IF NOT EXISTS idx_ultra_jobs_kind_updated ON ultra_jobs(kind, updated_at DESC)`);
  await dbPool.query(`CREATE INDEX IF NOT EXISTS idx_campaign_resume_ad_account ON campaign_resume(ad_account_id)`);
  await dbPool.query(`CREATE INDEX IF NOT EXISTS idx_campaign_resume_campaign_id ON campaign_resume(campaign_id)`);
}

async function loadDbState() {
  const [jobsResult, resumeResult] = await Promise.all([
    dbPool.query('SELECT id, data FROM ultra_jobs'),
    dbPool.query('SELECT key, data FROM campaign_resume')
  ]);

  const next = emptyState();
  next.storage = 'postgres';

  for (const row of jobsResult.rows) {
    if (row?.id && row?.data) next.jobs[row.id] = row.data;
  }

  for (const row of resumeResult.rows) {
    if (row?.key && row?.data) next.resume[row.key] = row.data;
  }

  next.updatedAt = new Date().toISOString();
  stateCache = normalizeState(next);
}

async function migrateFileStateToDbIfNeeded() {
  if (!fs.existsSync(STATE_FILE)) return;
  const fileState = readFileState();
  const jobs = Object.values(fileState.jobs || {});
  const resumes = Object.values(fileState.resume || {});
  if (!jobs.length && !resumes.length) return;

  for (const job of jobs) {
    if (!job?.id) continue;
    await dbPool.query(
      `INSERT INTO ultra_jobs (id, kind, status, data, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, NOW())
       ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, status = EXCLUDED.status, data = EXCLUDED.data, updated_at = NOW()`,
      [job.id, job.kind || null, job.status || null, JSON.stringify(job)]
    );
  }

  for (const record of resumes) {
    const adAccountId = normalizeAccountKey(record.adAccountId);
    const pageId = String(record.pageId || '').trim();
    if (!adAccountId || !pageId) continue;
    const key = makeResumeKey(adAccountId, pageId);
    const data = { ...record, key, adAccountId, pageId };
    await dbPool.query(
      `INSERT INTO campaign_resume (key, ad_account_id, page_id, campaign_id, data, updated_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
       ON CONFLICT (key) DO UPDATE SET
         ad_account_id = EXCLUDED.ad_account_id,
         page_id = EXCLUDED.page_id,
         campaign_id = EXCLUDED.campaign_id,
         data = campaign_resume.data || EXCLUDED.data,
         updated_at = NOW()`,
      [key, adAccountId, pageId, data.campaignId || null, JSON.stringify(data)]
    );
  }
}

export async function initUltraStore() {
  stateCache = readFileState();

  if (!DATABASE_URL) {
    dbReady = false;
    dbError = null;
    console.log('[ultraStore] DATABASE_URL not set. Using local file store:', STATE_FILE);
    return getUltraStoreStatus();
  }

  try {
    dbPool = new Pool({
      connectionString: DATABASE_URL,
      max: Number(process.env.PG_POOL_MAX || 5),
      idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
      connectionTimeoutMillis: Number(process.env.PG_CONNECTION_TIMEOUT_MS || 15000),
      ssl: shouldUseSslForDatabase(DATABASE_URL) ? { rejectUnauthorized: false } : false
    });

    await dbPool.query('SELECT 1');
    await ensureDbSchema();
    await migrateFileStateToDbIfNeeded();
    await loadDbState();
    dbReady = true;
    stateCache.storage = 'postgres';
    dbError = null;
    console.log('[ultraStore] Connected to Postgres. Job/resume storage is persistent.');
  } catch (err) {
    dbReady = false;
    dbError = err?.message || 'Postgres init failed';
    console.error('[ultraStore] Postgres init failed. Falling back to file store:', dbError);
    stateCache = readFileState();
  }

  return getUltraStoreStatus();
}

export function getUltraStoreStatus() {
  return {
    storage: dbReady ? 'postgres' : 'file',
    databaseConfigured: Boolean(DATABASE_URL),
    databaseReady: dbReady,
    databaseError: dbError,
    jobs: Object.keys(stateCache.jobs || {}).length,
    resumeRecords: Object.keys(stateCache.resume || {}).length,
    updatedAt: stateCache.updatedAt || null
  };
}

function persistFileCache() {
  if (!dbReady) writeFileState(stateCache);
}

function queueDbWrite(label, fn) {
  if (!dbReady || !dbPool) {
    persistFileCache();
    return;
  }

  writeQueue = writeQueue
    .then(fn)
    .catch((err) => {
      dbError = err?.message || String(err);
      console.error(`[ultraStore] DB write failed (${label}):`, dbError);
      // Giữ cache trong RAM và ghi fallback ra file để hạn chế mất state nếu DB ngắt tạm thời.
      try {
        writeFileState(stateCache);
      } catch (fileErr) {
        console.error('[ultraStore] File fallback write failed:', fileErr?.message || fileErr);
      }
    });
}

export function normalizeAccountKey(adAccountId) {
  return String(adAccountId || '').trim().replace(/^act_/, '');
}

export function makeResumeKey(adAccountId, pageId) {
  return `${normalizeAccountKey(adAccountId)}::${String(pageId || '').trim()}`;
}

export function readUltraState() {
  return cloneJson(stateCache);
}

export function writeUltraState(nextState) {
  stateCache = normalizeState(nextState);

  if (dbReady && dbPool) {
    const jobs = Object.values(stateCache.jobs || {});
    const resumes = Object.values(stateCache.resume || {});
    queueDbWrite('writeUltraState', async () => {
      await dbPool.query('BEGIN');
      try {
        await dbPool.query('DELETE FROM ultra_jobs');
        await dbPool.query('DELETE FROM campaign_resume');
        for (const job of jobs) {
          if (!job?.id) continue;
          await dbPool.query(
            `INSERT INTO ultra_jobs (id, kind, status, data, updated_at)
             VALUES ($1, $2, $3, $4::jsonb, NOW())`,
            [job.id, job.kind || null, job.status || null, JSON.stringify(job)]
          );
        }
        for (const record of resumes) {
          const adAccountId = normalizeAccountKey(record.adAccountId);
          const pageId = String(record.pageId || '').trim();
          if (!adAccountId || !pageId) continue;
          const key = makeResumeKey(adAccountId, pageId);
          await dbPool.query(
            `INSERT INTO campaign_resume (key, ad_account_id, page_id, campaign_id, data, updated_at)
             VALUES ($1, $2, $3, $4, $5::jsonb, NOW())`,
            [key, adAccountId, pageId, record.campaignId || null, JSON.stringify({ ...record, key, adAccountId, pageId })]
          );
        }
        await dbPool.query('COMMIT');
      } catch (err) {
        await dbPool.query('ROLLBACK');
        throw err;
      }
    });
  } else {
    persistFileCache();
  }

  return readUltraState();
}

export function updateUltraState(updater) {
  const current = readUltraState();
  const next = updater(current) || current;
  return writeUltraState(next);
}

export function saveUltraJob(job) {
  if (!job?.id) return job;
  const nextJob = cloneJson({ ...job, updatedAt: new Date().toISOString() });
  stateCache.jobs[nextJob.id] = nextJob;
  stateCache.updatedAt = new Date().toISOString();

  queueDbWrite('saveUltraJob', async () => {
    await dbPool.query(
      `INSERT INTO ultra_jobs (id, kind, status, data, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, NOW())
       ON CONFLICT (id) DO UPDATE SET
         kind = EXCLUDED.kind,
         status = EXCLUDED.status,
         data = EXCLUDED.data,
         updated_at = NOW()`,
      [nextJob.id, nextJob.kind || null, nextJob.status || null, JSON.stringify(nextJob)]
    );
  });

  return job;
}

export function getUltraJob(jobId) {
  const job = stateCache.jobs?.[jobId] || null;
  return job ? cloneJson(job) : null;
}

export function listUltraJobs({ kind, limit = 20 } = {}) {
  return Object.values(stateCache.jobs || {})
    .filter((job) => !kind || job.kind === kind)
    .sort((a, b) => String(b.updatedAt || b.createdAt || '').localeCompare(String(a.updatedAt || a.createdAt || '')))
    .slice(0, limit)
    .map(cloneJson);
}

export function upsertResumeRecord(record) {
  const adAccountId = normalizeAccountKey(record.adAccountId);
  const pageId = String(record.pageId || '').trim();
  if (!adAccountId || !pageId) return null;

  const key = makeResumeKey(adAccountId, pageId);
  const existing = stateCache.resume[key] || {};
  const nextRecord = {
    ...existing,
    key,
    adAccountId,
    pageId,
    pageName: record.pageName || existing.pageName || pageId,
    campaignId: record.campaignId || existing.campaignId || null,
    campaignName: record.campaignName || existing.campaignName || null,
    adSetId: record.adSetId || existing.adSetId || null,
    adId: record.adId || existing.adId || null,
    adsManagerUrl: record.adsManagerUrl || existing.adsManagerUrl || null,
    status: record.status || existing.status || 'success',
    source: record.source || existing.source || 'job',
    updatedAt: new Date().toISOString(),
    createdAt: existing.createdAt || record.createdAt || new Date().toISOString(),
    raw: record.raw || existing.raw || null
  };

  stateCache.resume[key] = nextRecord;
  stateCache.updatedAt = new Date().toISOString();

  queueDbWrite('upsertResumeRecord', async () => {
    await dbPool.query(
      `INSERT INTO campaign_resume (key, ad_account_id, page_id, campaign_id, data, updated_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
       ON CONFLICT (key) DO UPDATE SET
         ad_account_id = EXCLUDED.ad_account_id,
         page_id = EXCLUDED.page_id,
         campaign_id = EXCLUDED.campaign_id,
         data = campaign_resume.data || EXCLUDED.data,
         updated_at = NOW()`,
      [key, adAccountId, pageId, nextRecord.campaignId || null, JSON.stringify(nextRecord)]
    );
  });

  return cloneJson(nextRecord);
}

export function bulkUpsertResumeRecords(records = []) {
  const saved = [];
  for (const record of records) {
    const next = upsertResumeRecord(record);
    if (next) saved.push(next);
  }
  return saved;
}

export function getResumeRecord(adAccountId, pageId) {
  const record = stateCache.resume?.[makeResumeKey(adAccountId, pageId)] || null;
  return record ? cloneJson(record) : null;
}

export function getResumeRecordsForAdAccount(adAccountId) {
  const accountKey = normalizeAccountKey(adAccountId);
  return Object.values(stateCache.resume || {})
    .filter((record) => record.adAccountId === accountKey)
    .map(cloneJson);
}

export function clearResumeRecordsForAdAccount(adAccountId) {
  const accountKey = normalizeAccountKey(adAccountId);
  const keys = Object.entries(stateCache.resume || {})
    .filter(([, record]) => record.adAccountId === accountKey)
    .map(([key]) => key);

  for (const key of keys) delete stateCache.resume[key];
  stateCache.updatedAt = new Date().toISOString();

  queueDbWrite('clearResumeRecordsForAdAccount', async () => {
    await dbPool.query('DELETE FROM campaign_resume WHERE ad_account_id = $1', [accountKey]);
  });

  return keys.length;
}
