import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { appendJobs, readJobs, writeJobs } from './jobStore.js';
import {
  saveUltraJob,
  getUltraJob,
  listUltraJobs,
  upsertResumeRecord,
  bulkUpsertResumeRecords,
  getResumeRecord,
  getResumeRecordsForAdAccount,
  clearResumeRecordsForAdAccount,
  normalizeAccountKey,
  initUltraStore,
  getUltraStoreStatus
} from './ultraStore.js';
import {
  createCampaignDraft,
  createAdSetDraft,
  createAdDraft,
  createAdDraftWithObjectStoryId,
  pickFirstValidPostAndCreateAd,
  updateCampaignStatus,
  updateAdSetStatus,
  updateAdStatus,
  getAdAccount,
  getAdSet,
  getAd,
  listCampaigns,
  listAllCampaigns,
  getCampaign,
  deleteCampaign,
  scanPermissions,
  classifyMetaError,
  requestPageAccessToBusiness,
  listBusinessOwnedPages
} from './metaApi.js';
import {
  readTokenStore,
  saveUserToken,
  clearStoredUserToken
} from './tokenStore.js';

const app = express();
const port = Number(process.env.PORT || 3000);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const FB_APP_ID = process.env.META_APP_ID || process.env.FB_APP_ID || '';
const FB_APP_SECRET = process.env.META_APP_SECRET || process.env.FB_APP_SECRET || '';
const FB_REDIRECT_URI =
  process.env.META_REDIRECT_URI || `http://localhost:${port}/auth/facebook/callback`;

app.use(express.static(path.join(__dirname, '../public')));
app.use(cors());
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '50mb' }));
app.use(express.urlencoded({ extended: true, limit: process.env.JSON_BODY_LIMIT || '50mb' }));

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});
function buildAdsManagerUrl({ adAccountId, campaignId, adSetId, adId }) {
  const numericAccountId = String(adAccountId || '').replace(/^act_/, '');
  let url = `https://adsmanager.facebook.com/adsmanager/manage/campaigns?act=${numericAccountId}`;
  if (campaignId) url += `&selected_campaign_ids=${campaignId}`;
  if (adSetId) url += `&selected_adset_ids=${adSetId}`;
  if (adId) url += `&selected_ad_ids=${adId}`;
  return url;
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, store: getUltraStoreStatus() });
});

app.get('/accounts/:adAccountId', async (req, res) => {
  try {
    const data = await getAdAccount(req.params.adAccountId);
    res.json(data);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});


app.post('/permissions/scan', async (req, res) => {
  try {
    const { adAccountId, pageIds } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!Array.isArray(pageIds)) return res.status(400).json({ error: 'Missing pageIds' });

    const data = await scanPermissions({ adAccountId, pageIds });
    res.json({ ok: true, ...data });
  } catch (err) {
    res.status(400).json({
      ok: false,
      error: err.message,
      errorType: err.errorType || classifyMetaError(err.message)
    });
  }
});


app.post('/permissions/request-page-access', async (req, res) => {
  try {
    const {
      businessId,
      pageIds = [],
      permittedTasks = ['ADVERTISE', 'ANALYZE', 'CREATE_CONTENT'],
      mode = 'client'
    } = req.body || {};

    if (!businessId) {
      return res.status(400).json({
        ok: false,
        error: 'Missing businessId'
      });
    }

    if (!Array.isArray(pageIds) || !pageIds.length) {
      return res.status(400).json({
        ok: false,
        error: 'Missing pageIds'
      });
    }

    const result = await requestPageAccessToBusiness({
      businessId,
      pageIds,
      permittedTasks,
      mode
    });

    return res.json(result);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message || 'Request page access failed',
      errorType: err.errorType || 'UNKNOWN',
      meta: err.meta || null
    });
  }
});


app.get('/business/:businessId/owned-pages', async (req, res) => {
  try {
    const pages = await listBusinessOwnedPages({ businessId: req.params.businessId });
    return res.json({
      ok: true,
      businessId: req.params.businessId,
      total: pages.length,
      pages
    });
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || 'Cannot list business owned pages',
      errorType: err.errorType || 'UNKNOWN',
      meta: err.meta || null
    });
  }
});

app.get('/campaigns', async (req, res) => {
  try {
    const { adAccountId } = req.query;

    if (!adAccountId) {
      return res.status(400).json({ error: 'Missing adAccountId' });
    }

    const data = await listCampaigns(adAccountId);
    res.json(data);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/flow/create-campaign-and-adset-draft', async (req, res) => {
  try {
    const {
      adAccountId,
      campaignName,
      adSetName,
      objective,
      dailyBudget,
      pageId,
      optimizationGoal
    } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!campaignName) return res.status(400).json({ error: 'Missing campaignName' });
    if (!adSetName) return res.status(400).json({ error: 'Missing adSetName' });
    if (!pageId) return res.status(400).json({ error: 'Missing pageId' });

    const campaign = await createCampaignDraft({
      adAccountId,
      campaignName,
      objective,
      dailyBudget
    });

    const adSet = await createAdSetDraft({
      adAccountId,
      campaignId: campaign.id,
      adSetName,
      pageId,
      optimizationGoal
    });

    const adSetDetail = await getAdSet(adSet.id);
    const adsManagerUrl = buildAdsManagerUrl({
      adAccountId,
      campaignId: campaign.id,
      adSetId: adSet.id
    });

    res.json({
      ok: true,
      campaign: { id: campaign.id, name: campaignName },
      adSet: {
        id: adSetDetail.id,
        name: adSetDetail.name,
        status: adSetDetail.status,
        optimization_goal: adSetDetail.optimization_goal,
        destination_type: adSetDetail.destination_type
      },
      adsManagerUrl
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});


function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampNumber(value, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

async function runFullDraftCore(payload = {}) {
  const {
    adAccountId,
    campaignName,
    adSetName,
    adName,
    objective,
    dailyBudget,
    pageId,
    optimizationGoal,
    postId,
    audienceName,
    placements
  } = payload;

  if (!adAccountId) throw new Error('Missing adAccountId');
  if (!campaignName) throw new Error('Missing campaignName');
  if (!adSetName) throw new Error('Missing adSetName');
  if (!pageId) throw new Error('Missing pageId');
  if (!dailyBudget || Number(dailyBudget) <= 0) {
    throw new Error('Invalid dailyBudget');
  }

  const campaign = await createCampaignDraft({
    adAccountId,
    campaignName,
    objective,
    dailyBudget
  });

  const adSet = await createAdSetDraft({
    adAccountId,
    campaignId: campaign.id,
    adSetName,
    pageId,
    optimizationGoal
  });

  const adSetDetail = await getAdSet(adSet.id);

  let adDetail = null;
  let pickedPost = null;

  if (postId) {
    const ad = await createAdDraftWithObjectStoryId({
      adAccountId,
      adSetId: adSet.id,
      adName: adName || `Ad - ${pageId}`,
      objectStoryId: postId
    });

    adDetail = await getAd(ad.id);
    pickedPost = {
      source: 'input_post_id',
      id: postId
    };
  } else {
    const picked = await pickFirstValidPostAndCreateAd({
      adAccountId,
      adSetId: adSet.id,
      adName: adName || `Ad - ${pageId}`,
      pageId,
      limit: 10
    });

    adDetail = await getAd(picked.ad.id);
    pickedPost = picked.pickedPost;
  }

  if (adDetail?.id) {
    await updateCampaignStatus({
      campaignId: campaign.id,
      status: 'ACTIVE'
    });

    await updateAdSetStatus({
      adSetId: adSet.id,
      status: 'ACTIVE'
    });

    await updateAdStatus({
      adId: adDetail.id,
      status: 'ACTIVE'
    });
  }

  const adsManagerUrl = buildAdsManagerUrl({
    adAccountId,
    campaignId: campaign.id,
    adSetId: adSet.id,
    adId: adDetail?.id
  });

  return {
    ok: true,
    campaign: {
      id: campaign.id,
      name: campaignName,
      status: adDetail?.id ? 'ACTIVE' : 'PAUSED'
    },
    adSet: {
      id: adSetDetail.id,
      name: adSetDetail.name,
      status: adDetail?.id ? 'ACTIVE' : adSetDetail.status,
      optimization_goal: adSetDetail.optimization_goal,
      destination_type: adSetDetail.destination_type,
      daily_budget: dailyBudget
    },
    ad: adDetail
      ? {
          id: adDetail.id,
          name: adDetail.name,
          status: 'ACTIVE',
          adset_id: adDetail.adset_id,
          campaign_id: adDetail.campaign_id
        }
      : null,
    pickedPost,
    adsManagerUrl,
    uiPlan: {
      audienceName: audienceName || null,
      placements: placements || null,
      hasPostSelectionStep: false,
      hasPublishStep: false
    },
    publishResult: adDetail?.id
      ? {
          campaignStatus: 'ACTIVE',
          adSetStatus: 'ACTIVE',
          adStatus: 'ACTIVE'
        }
      : null
  };
}

app.post('/flow/run-full-draft', async (req, res) => {
  try {
    const data = await runFullDraftCore(req.body || {});
    res.json(data);
  } catch (err) {
    res.status(400).json({
      ok: false,
      error: err.message,
      errorType: err.errorType || classifyMetaError(err.message),
      meta: err.meta || null
    });
  }
});


const runtimeJobs = new Map();
const MAX_JOB_EVENTS = 5000;
const MAX_FINISHED_JOBS = 50;
const DEFAULT_RATE_LIMIT_COOLDOWN_MS = clampNumber(process.env.RATE_LIMIT_COOLDOWN_MS, 10 * 60 * 1000, 60 * 1000, 60 * 60 * 1000);
const MAX_RATE_LIMIT_COOLDOWN_MS = clampNumber(process.env.MAX_RATE_LIMIT_COOLDOWN_MS, 30 * 60 * 1000, 5 * 60 * 1000, 2 * 60 * 60 * 1000);

function makeJobId() {
  return `job_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function persistJob(job) {
  job.updatedAt = new Date().toISOString();
  runtimeJobs.set(job.id, job);
  saveUltraJob(job);
  return job;
}

function getJobAny(jobId) {
  const runtime = runtimeJobs.get(jobId);
  if (runtime) return runtime;
  const stored = getUltraJob(jobId);
  if (!stored) return null;
  runtimeJobs.set(jobId, stored);
  return stored;
}

function publicJob(job) {
  return {
    id: job.id,
    kind: job.kind,
    status: job.status,
    total: job.total,
    completed: job.completed,
    success: job.success,
    failed: job.failed,
    skipped: job.skipped || 0,
    partial: job.partial || 0,
    concurrency: job.concurrency,
    delayMs: job.delayMs,
    maxRetries: job.maxRetries,
    startedAt: job.startedAt,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    finishedAt: job.finishedAt || null,
    error: job.error || null,
    rateLimit: job.rateLimit || null,
    autoResume: job.autoResume !== false,
    publishActive: Boolean(job.publishActive),
    progressText: `${job.completed || 0}/${job.total || 0}`
  };
}

function pushJobEvent(job, event, { persist = true } = {}) {
  job.events = Array.isArray(job.events) ? job.events : [];
  job.nextEventIndex = Number(job.nextEventIndex || 0);
  job.events.push({
    eventIndex: job.nextEventIndex++,
    at: new Date().toISOString(),
    ...event
  });

  if (job.events.length > MAX_JOB_EVENTS) {
    job.events.splice(0, job.events.length - MAX_JOB_EVENTS);
  }
  if (persist) persistJob(job);
}

function classifyJobError(err) {
  return err?.errorType || classifyMetaError(err?.message || '') || 'UNKNOWN';
}

function isRateLimitJobError(err) {
  const msg = String(err?.message || '').toLowerCase();
  const type = String(err?.errorType || '').toUpperCase();
  const code = Number(err?.meta?.code);
  const subcode = Number(err?.meta?.error_subcode || err?.meta?.subcode);
  return (
    type === 'RATE_LIMIT' ||
    [4, 17, 32, 613, 80004].includes(code) ||
    [2446079, 1487390].includes(subcode) ||
    msg.includes('application request limit reached') ||
    msg.includes('rate limit') ||
    msg.includes('too many calls') ||
    msg.includes('temporarily blocked') ||
    msg.includes('try again later') ||
    msg.includes('please reduce the amount of data')
  );
}

function isRetryableNetworkJobError(err) {
  const msg = String(err?.message || '').toLowerCase();
  return (
    msg.includes('timeout') ||
    msg.includes('network') ||
    msg.includes('fetch failed') ||
    msg.includes('connection') ||
    msg.includes('econnreset') ||
    msg.includes('etimedout') ||
    msg.includes('socket')
  );
}

function retryDelayMs(attempt, baseDelayMs) {
  const base = Math.max(1000, Number(baseDelayMs || 1000));
  const jitter = Math.floor(Math.random() * 1000);
  return Math.min(120000, base * Math.pow(2, attempt)) + jitter;
}

function getRateLimitCooldownMs(job) {
  const base = clampNumber(job.rateLimitCooldownMs, DEFAULT_RATE_LIMIT_COOLDOWN_MS, 60 * 1000, 2 * 60 * 60 * 1000);
  const hitCount = Number(job.rateLimit?.hitCount || 0);
  const multiplier = Math.min(4, Math.max(1, hitCount));
  const jitter = Math.floor(Math.random() * 30000);
  return Math.min(MAX_RATE_LIMIT_COOLDOWN_MS, base * multiplier) + jitter;
}

function markGlobalRateLimit(job, err, { workerId, item } = {}) {
  const cooldownMs = getRateLimitCooldownMs(job);
  const cooldownUntilMs = Date.now() + cooldownMs;
  job.status = 'rate_limited';
  job.rateLimit = {
    active: true,
    hitCount: Number(job.rateLimit?.hitCount || 0) + 1,
    cooldownMs,
    cooldownUntil: new Date(cooldownUntilMs).toISOString(),
    lastError: err?.message || 'Rate limit',
    lastMeta: err?.meta || null
  };

  if (item && item.status === 'processing') {
    item.status = 'pending';
    item.workerId = null;
    item.lastError = err?.message || 'Rate limit';
    item.errorType = 'RATE_LIMIT';
  }

  pushJobEvent(job, {
    type: 'rate_limit',
    workerId,
    pageId: item?.payload?.pageId || null,
    message: `⚠️ Gặp Meta rate limit (#4). Tạm dừng toàn bộ job ${Math.round(cooldownMs / 60000)} phút, không đánh FAILED và sẽ tự chạy tiếp.`
  });
}

async function waitForManualPauseOrRateLimit(job, workerId) {
  while (true) {
    if (job.cancelRequested || job.status === 'cancelled') return false;

    if (job.status === 'paused') {
      await sleep(3000);
      continue;
    }

    if (job.status === 'rate_limited') {
      const until = Date.parse(job.rateLimit?.cooldownUntil || '');
      const waitMs = Number.isFinite(until) ? until - Date.now() : 60_000;
      if (waitMs > 0) {
        await sleep(Math.min(waitMs, 30_000));
        continue;
      }

      // Hết thời gian cooldown: test bằng 1 request nhẹ trước khi chạy tiếp.
      try {
        pushJobEvent(job, {
          type: 'running',
          workerId,
          message: `⏳ Hết cooldown, test quota bằng request nhẹ trước khi resume...`
        });
        const adAccountId = job.adAccountId || job.payloads?.[0]?.adAccountId;
        if (adAccountId) await getAdAccount(adAccountId);
        job.status = 'running';
        job.rateLimit = {
          ...(job.rateLimit || {}),
          active: false,
          resumedAt: new Date().toISOString()
        };
        pushJobEvent(job, {
          type: 'section',
          workerId,
          message: `✅ Quota đã hồi. Job tự chạy tiếp từ dòng chưa xong.`
        });
        persistJob(job);
        return true;
      } catch (err) {
        if (isRateLimitJobError(err)) {
          markGlobalRateLimit(job, err, { workerId });
          continue;
        }
        // Lỗi test không phải limit thì đừng kẹt vô hạn.
        job.status = 'running';
        pushJobEvent(job, {
          type: 'running',
          workerId,
          message: `⚠️ Test quota lỗi không phải limit (${err.message || 'unknown'}), thử chạy tiếp thận trọng.`
        });
        persistJob(job);
        return true;
      }
    }

    return true;
  }
}

function hasPendingItems(job) {
  return (job.items || []).some((item) => item.status === 'pending');
}

function claimNextItem(job, workerId) {
  const item = (job.items || []).find((x) => x.status === 'pending');
  if (!item) return null;
  item.status = 'processing';
  item.workerId = workerId;
  item.startedAt = new Date().toISOString();
  return item;
}

function buildSuccessResult({ job, payload, checkpoint, adSetDetail, adDetail, pickedPost }) {
  const adsManagerUrl = buildAdsManagerUrl({
    adAccountId: payload.adAccountId,
    campaignId: checkpoint.campaignId,
    adSetId: checkpoint.adSetId,
    adId: checkpoint.adId
  });

  return {
    ok: true,
    campaign: {
      id: checkpoint.campaignId,
      name: payload.campaignName,
      status: job.publishActive ? 'ACTIVE' : 'PAUSED'
    },
    adSet: {
      id: checkpoint.adSetId,
      name: adSetDetail?.name || payload.adSetName,
      status: job.publishActive ? 'ACTIVE' : (adSetDetail?.status || 'PAUSED'),
      optimization_goal: adSetDetail?.optimization_goal || payload.optimizationGoal,
      destination_type: adSetDetail?.destination_type || 'MESSENGER',
      daily_budget: payload.dailyBudget
    },
    ad: checkpoint.adId
      ? {
          id: checkpoint.adId,
          name: adDetail?.name || payload.adName,
          status: job.publishActive ? 'ACTIVE' : (adDetail?.status || 'PAUSED'),
          adset_id: checkpoint.adSetId,
          campaign_id: checkpoint.campaignId
        }
      : null,
    pickedPost: pickedPost || checkpoint.pickedPost || null,
    adsManagerUrl,
    publishResult: job.publishActive
      ? { campaignStatus: 'ACTIVE', adSetStatus: 'ACTIVE', adStatus: 'ACTIVE' }
      : { campaignStatus: 'PAUSED', adSetStatus: 'PAUSED', adStatus: 'PAUSED' }
  };
}

async function runFullItemCheckpointed({ job, item, workerId }) {
  const payload = item.payload;
  const label = payload.pageName || payload.pageId || `Dòng ${item.index + 1}`;
  item.checkpoint = item.checkpoint || {};
  const checkpoint = item.checkpoint;

  // Backend resume chống trùng: nếu từng có campaign/adset/ad thì reuse, không tạo lại camp.
  const existing = getResumeRecord(payload.adAccountId, payload.pageId);
  if (existing?.campaignId && !checkpoint.campaignId) {
    checkpoint.campaignId = existing.campaignId;
    checkpoint.adSetId = existing.adSetId || null;
    checkpoint.adId = existing.adId || null;
    checkpoint.reusedFromResume = true;
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Reuse campaign đã có trong resume (${existing.campaignId})`
    });
  }

  if (existing?.status === 'success' && existing?.campaignId && existing?.adSetId && existing?.adId) {
    const result = buildSuccessResult({ job, payload, checkpoint: { ...checkpoint, ...existing }, adSetDetail: null, adDetail: null });
    return { skipped: true, result, checkpoint: { ...checkpoint, ...existing } };
  }

  if (!checkpoint.campaignId) {
    const campaign = await createCampaignDraft({
      adAccountId: payload.adAccountId,
      campaignName: payload.campaignName,
      objective: payload.objective,
      dailyBudget: payload.dailyBudget
    });
    checkpoint.campaignId = campaign.id;
    checkpoint.campaignCreatedAt = new Date().toISOString();
    upsertResumeRecord({
      adAccountId: payload.adAccountId,
      pageId: payload.pageId,
      pageName: payload.pageName,
      campaignId: campaign.id,
      campaignName: payload.campaignName,
      status: 'campaign_created',
      source: 'job_partial',
      adsManagerUrl: buildAdsManagerUrl({ adAccountId: payload.adAccountId, campaignId: campaign.id })
    });
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Đã tạo campaign ${campaign.id}, lưu checkpoint`
    });
    persistJob(job);
  }

  if (!checkpoint.adSetId) {
    const adSet = await createAdSetDraft({
      adAccountId: payload.adAccountId,
      campaignId: checkpoint.campaignId,
      adSetName: payload.adSetName,
      pageId: payload.pageId,
      optimizationGoal: payload.optimizationGoal
    });
    checkpoint.adSetId = adSet.id;
    checkpoint.adSetCreatedAt = new Date().toISOString();
    upsertResumeRecord({
      adAccountId: payload.adAccountId,
      pageId: payload.pageId,
      pageName: payload.pageName,
      campaignId: checkpoint.campaignId,
      campaignName: payload.campaignName,
      adSetId: checkpoint.adSetId,
      status: 'adset_created',
      source: 'job_partial',
      adsManagerUrl: buildAdsManagerUrl({ adAccountId: payload.adAccountId, campaignId: checkpoint.campaignId, adSetId: checkpoint.adSetId })
    });
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Đã tạo adset ${adSet.id}, lưu checkpoint`
    });
    persistJob(job);
  }

  let adSetDetail = null;
  try {
    adSetDetail = await getAdSet(checkpoint.adSetId);
  } catch (err) {
    if (isRateLimitJobError(err)) throw err;
    // Không chặn job chỉ vì verify adset bị lỗi nhẹ.
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Không đọc lại được adset, vẫn chạy tiếp: ${err.message || 'verify lỗi'}`
    });
  }

  let pickedPost = checkpoint.pickedPost || null;
  let adDetail = null;

  if (!checkpoint.adId) {
    if (payload.postId) {
      const ad = await createAdDraftWithObjectStoryId({
        adAccountId: payload.adAccountId,
        adSetId: checkpoint.adSetId,
        adName: payload.adName || `Ad - ${payload.pageId}`,
        objectStoryId: payload.postId
      });
      checkpoint.adId = ad.id;
      pickedPost = { source: 'input_post_id', id: payload.postId };
      checkpoint.pickedPost = pickedPost;
    } else {
      const picked = await pickFirstValidPostAndCreateAd({
        adAccountId: payload.adAccountId,
        adSetId: checkpoint.adSetId,
        adName: payload.adName || `Ad - ${payload.pageId}`,
        pageId: payload.pageId,
        limit: 10
      });
      checkpoint.adId = picked.ad.id;
      pickedPost = picked.pickedPost;
      checkpoint.pickedPost = pickedPost;
    }

    checkpoint.adCreatedAt = new Date().toISOString();
    upsertResumeRecord({
      adAccountId: payload.adAccountId,
      pageId: payload.pageId,
      pageName: payload.pageName,
      campaignId: checkpoint.campaignId,
      campaignName: payload.campaignName,
      adSetId: checkpoint.adSetId,
      adId: checkpoint.adId,
      status: 'ad_created',
      source: 'job_partial',
      adsManagerUrl: buildAdsManagerUrl({ adAccountId: payload.adAccountId, campaignId: checkpoint.campaignId, adSetId: checkpoint.adSetId, adId: checkpoint.adId })
    });
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Đã tạo ad ${checkpoint.adId}, lưu checkpoint`
    });
    persistJob(job);
  }

  try {
    adDetail = await getAd(checkpoint.adId);
  } catch (err) {
    if (isRateLimitJobError(err)) throw err;
    pushJobEvent(job, {
      type: 'running',
      workerId,
      index: item.index,
      pageId: payload.pageId,
      message: `[Bàn ${workerId}] ${label} - Không đọc lại được ad, vẫn lưu success: ${err.message || 'verify lỗi'}`
    });
  }

  if (job.publishActive) {
    await updateCampaignStatus({ campaignId: checkpoint.campaignId, status: 'ACTIVE' });
    await updateAdSetStatus({ adSetId: checkpoint.adSetId, status: 'ACTIVE' });
    if (checkpoint.adId) await updateAdStatus({ adId: checkpoint.adId, status: 'ACTIVE' });
  }

  const result = buildSuccessResult({ job, payload, checkpoint, adSetDetail, adDetail, pickedPost });
  upsertResumeRecord({
    adAccountId: payload.adAccountId,
    pageId: payload.pageId,
    pageName: payload.pageName,
    campaignId: checkpoint.campaignId,
    campaignName: payload.campaignName,
    adSetId: checkpoint.adSetId,
    adId: checkpoint.adId,
    status: 'success',
    source: 'job_success',
    adsManagerUrl: result.adsManagerUrl
  });

  return { skipped: false, result, checkpoint };
}

async function processFullItemWithSmartRetry({ job, item, workerId }) {
  let lastErr = null;
  for (let attempt = Number(item.attempts || 0); attempt <= job.maxRetries; attempt += 1) {
    item.attempts = attempt;
    try {
      return await runFullItemCheckpointed({ job, item, workerId });
    } catch (err) {
      lastErr = err;
      item.lastError = err.message || 'Lỗi backend';
      item.errorType = classifyJobError(err);

      if (isRateLimitJobError(err)) {
        markGlobalRateLimit(job, err, { workerId, item });
        return { rateLimited: true };
      }

      const retryable = isRetryableNetworkJobError(err);
      if (!retryable || attempt >= job.maxRetries) throw err;

      const waitMs = retryDelayMs(attempt, job.delayMs);
      pushJobEvent(job, {
        type: 'running',
        index: item.index,
        workerId,
        pageId: item.payload.pageId,
        message: `[Bàn ${workerId}] ${item.payload.pageName || item.payload.pageId} lỗi mạng/tạm thời, retry lần ${attempt + 1}/${job.maxRetries} sau ${Math.round(waitMs / 1000)}s: ${err.message || 'lỗi API'}`
      });
      await sleep(waitMs);
    }
  }
  throw lastErr || new Error('Unknown job error');
}

async function runFullFlowJob(jobId) {
  const job = getJobAny(jobId);
  if (!job || job.runnerActive) return;
  job.runnerActive = true;
  job.status = job.status === 'queued' ? 'running' : job.status;
  job.startedAt = job.startedAt || new Date().toISOString();
  pushJobEvent(job, {
    type: 'section',
    message: `Job bắt đầu/tiếp tục: ${job.total} dòng | ${job.concurrency} bàn làm việc | mỗi bàn 1 ID/lần | delay ${job.delayMs}ms | retry ${job.maxRetries} | rate-limit auto pause ${Math.round((job.rateLimitCooldownMs || DEFAULT_RATE_LIMIT_COOLDOWN_MS) / 60000)} phút | campaign ${job.publishActive ? 'ACTIVE' : 'PAUSED'}`
  });

  async function worker(workerId) {
    while (true) {
      if (!(await waitForManualPauseOrRateLimit(job, workerId))) return;
      if (job.cancelRequested || job.status === 'cancelled') return;
      if (job.status !== 'running') return;

      const item = claimNextItem(job, workerId);
      if (!item) return;

      const payload = item.payload;
      const label = payload.pageName || payload.pageId || `Dòng ${item.index + 1}`;

      if (job.delayMs > 0 && item.index > 0) {
        await sleep(job.delayMs);
      }

      pushJobEvent(job, {
        type: 'running',
        index: item.index,
        workerId,
        pageId: payload.pageId,
        pageName: payload.pageName || payload.pageId,
        message: `[Bàn ${workerId}] Nhận dòng ${item.index + 1}/${job.total}: ${label} (${payload.pageId})`
      });

      try {
        const data = await processFullItemWithSmartRetry({ job, item, workerId });
        if (data?.rateLimited) continue;

        item.status = data.skipped ? 'skipped' : 'success';
        item.finishedAt = new Date().toISOString();
        item.result = data.result;
        item.checkpoint = data.checkpoint || item.checkpoint;
        job.completed += 1;
        if (data.skipped) job.skipped += 1;
        else job.success += 1;
        job.results[item.index] = {
          ok: true,
          skipped: Boolean(data.skipped),
          index: item.index,
          pageId: payload.pageId,
          pageName: payload.pageName || payload.pageId,
          payload,
          result: data.result
        };
        pushJobEvent(job, {
          type: data.skipped ? 'skipped' : 'success',
          index: item.index,
          pageId: payload.pageId,
          pageName: payload.pageName || payload.pageId,
          payload,
          result: data.result,
          adsManagerUrl: data.result?.adsManagerUrl || null,
          message: `[Bàn ${workerId}] ${label} - ${data.skipped ? 'SKIP backend resume' : 'Thành công'}`
        });
      } catch (err) {
        item.status = 'failed';
        item.finishedAt = new Date().toISOString();
        item.error = err.message || 'Lỗi backend';
        item.errorType = classifyJobError(err);
        job.completed += 1;
        job.failed += 1;
        job.results[item.index] = {
          ok: false,
          index: item.index,
          pageId: payload.pageId,
          pageName: payload.pageName || payload.pageId,
          payload,
          checkpoint: item.checkpoint || {},
          error: err.message || 'Lỗi backend',
          errorType: item.errorType,
          meta: err.meta || null
        };
        pushJobEvent(job, {
          type: 'error',
          index: item.index,
          pageId: payload.pageId,
          pageName: payload.pageName || payload.pageId,
          payload,
          error: err.message || 'Lỗi backend',
          errorType: item.errorType,
          message: `[Bàn ${workerId}] ${label} - ${err.message || 'Lỗi backend'}${item.errorType ? ` [${item.errorType}]` : ''}`
        });
      }
      persistJob(job);
    }
  }

  try {
    const workers = Array.from({ length: Math.min(job.concurrency, job.items.filter((x) => x.status === 'pending').length || 1) }, (_, i) => worker(i + 1));
    await Promise.all(workers);

    if (job.cancelRequested || job.status === 'cancelled') {
      job.status = 'cancelled';
      job.finishedAt = new Date().toISOString();
      pushJobEvent(job, { type: 'section', message: `Job đã dừng theo yêu cầu. SUCCESS ${job.success} | FAILED ${job.failed} | SKIPPED ${job.skipped} | TOTAL ${job.total}` });
    } else if (job.status === 'paused' || job.status === 'rate_limited') {
      pushJobEvent(job, { type: 'section', message: `Job đang ${job.status === 'paused' ? 'tạm dừng thủ công' : 'chờ hết rate limit'}, có thể resume tiếp.` });
    } else if (!hasPendingItems(job)) {
      job.status = 'done';
      job.finishedAt = new Date().toISOString();
      pushJobEvent(job, { type: 'section', message: `Job kết thúc: SUCCESS ${job.success} | SKIPPED ${job.skipped} | FAILED ${job.failed} | TOTAL ${job.total}` });
    }
  } catch (err) {
    job.status = 'failed';
    job.error = err.message || 'Job failed';
    job.finishedAt = new Date().toISOString();
    pushJobEvent(job, { type: 'error', message: `Job lỗi: ${job.error}` });
  } finally {
    job.runnerActive = false;
    persistJob(job);
  }
}

function parseAutoPageIdFromCampaignName(name = '') {
  const text = String(name || '');
  if (!/^AUTO\s+/i.test(text)) return null;
  const nums = [...text.matchAll(/\d{8,}/g)].map((m) => m[0]);
  if (!nums.length) return null;
  return nums[nums.length - 1];
}

app.post('/flow/sync-existing-campaigns', async (req, res) => {
  try {
    const { adAccountId, pageIds = [], clearBeforeSync = false } = req.body || {};
    if (!adAccountId) return res.status(400).json({ ok: false, error: 'Missing adAccountId' });

    const wanted = new Set((Array.isArray(pageIds) ? pageIds : []).map((x) => String(x || '').trim()).filter(Boolean));
    if (clearBeforeSync) clearResumeRecordsForAdAccount(adAccountId);

    const campaigns = await listAllCampaigns(adAccountId);
    const matches = [];

    for (const c of campaigns) {
      const pageId = parseAutoPageIdFromCampaignName(c.name);
      if (!pageId) continue;
      if (wanted.size && !wanted.has(pageId)) continue;
      matches.push({
        adAccountId,
        pageId,
        pageName: pageId,
        campaignId: c.id,
        campaignName: c.name,
        status: c.effective_status === 'DELETED' || c.status === 'DELETED' ? 'deleted' : 'campaign_existing',
        source: 'sync_existing_campaigns',
        raw: c,
        adsManagerUrl: buildAdsManagerUrl({ adAccountId, campaignId: c.id })
      });
    }

    const activeMatches = matches.filter((x) => x.status !== 'deleted');
    const saved = bulkUpsertResumeRecords(activeMatches);
    return res.json({
      ok: true,
      scanned: campaigns.length,
      matched: matches.length,
      saved: saved.length,
      deletedIgnored: matches.length - activeMatches.length,
      records: saved
    });
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || 'Sync existing campaigns failed',
      errorType: classifyJobError(err),
      meta: err.meta || null
    });
  }
});

app.get('/flow/resume-records', (req, res) => {
  const { adAccountId } = req.query;
  if (!adAccountId) return res.status(400).json({ ok: false, error: 'Missing adAccountId' });
  const records = getResumeRecordsForAdAccount(adAccountId);
  return res.json({ ok: true, total: records.length, records });
});

app.get('/flow/jobs', (req, res) => {
  const kind = req.query.kind || 'full_flow';
  const jobs = listUltraJobs({ kind, limit: clampNumber(req.query.limit, 20, 1, 100) }).map(publicJob);
  return res.json({ ok: true, jobs });
});

app.post('/flow/start-full-job', async (req, res) => {
  try {
    const { payloads, settings = {} } = req.body || {};
    if (!Array.isArray(payloads) || !payloads.length) {
      return res.status(400).json({ ok: false, error: 'Missing payloads' });
    }
    if (payloads.length > 5000) {
      return res.status(400).json({ ok: false, error: 'Quá nhiều dòng trong một job. Chia nhỏ tối đa 5000 dòng/job.' });
    }

    const concurrency = clampNumber(settings.concurrency ?? process.env.FULL_FLOW_CONCURRENCY, 4, 1, 8);
    const delayMs = clampNumber(settings.delayMs ?? process.env.FULL_FLOW_DELAY_MS, 3000, 0, 300000);
    const maxRetries = clampNumber(settings.maxRetries ?? process.env.FULL_FLOW_MAX_RETRIES, 2, 0, 10);
    const rateLimitCooldownMs = clampNumber(settings.rateLimitCooldownMs ?? process.env.RATE_LIMIT_COOLDOWN_MS, DEFAULT_RATE_LIMIT_COOLDOWN_MS, 60 * 1000, 2 * 60 * 60 * 1000);
    const publishActive = Boolean(settings.publishActive);

    const cleanPayloads = [];
    const seen = new Set();
    for (const payload of payloads) {
      const pageId = String(payload?.pageId || '').trim();
      const adAccountId = String(payload?.adAccountId || '').trim();
      if (!pageId || !adAccountId) continue;
      const key = `${normalizeAccountKey(adAccountId)}::${pageId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      cleanPayloads.push({ ...payload, pageId, adAccountId, publishActive });
    }

    const id = makeJobId();
    const job = {
      id,
      kind: 'full_flow',
      status: 'queued',
      adAccountId: cleanPayloads[0]?.adAccountId || null,
      payloads: cleanPayloads,
      items: cleanPayloads.map((payload, index) => {
        const existing = getResumeRecord(payload.adAccountId, payload.pageId);
        const shouldSkip = settings.skipBackendSuccess !== false && existing?.status === 'success' && existing?.campaignId && existing?.adSetId && existing?.adId;
        return {
          index,
          payload,
          status: shouldSkip ? 'pending' : 'pending',
          attempts: 0,
          checkpoint: existing?.campaignId ? {
            campaignId: existing.campaignId,
            adSetId: existing.adSetId || null,
            adId: existing.adId || null,
            reusedFromResume: true
          } : {},
          existingResume: existing || null
        };
      }),
      total: cleanPayloads.length,
      completed: 0,
      success: 0,
      failed: 0,
      skipped: Number(settings.skipped || 0),
      partial: 0,
      concurrency,
      delayMs,
      maxRetries,
      rateLimitCooldownMs,
      publishActive,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      startedAt: null,
      finishedAt: null,
      cancelRequested: false,
      pauseRequested: false,
      autoResume: true,
      runnerActive: false,
      error: null,
      rateLimit: null,
      results: new Array(cleanPayloads.length),
      events: [],
      nextEventIndex: 0
    };

    persistJob(job);
    setImmediate(() => runFullFlowJob(id));
    return res.json({ ok: true, job: publicJob(job) });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message || 'Cannot start job',
      errorType: classifyJobError(err),
      meta: err.meta || null
    });
  }
});

app.get('/flow/job-status/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job) return res.status(404).json({ ok: false, error: 'Job not found' });

  if ((job.status === 'queued' || job.status === 'rate_limited' || job.status === 'running') && !job.runnerActive && hasPendingItems(job)) {
    setImmediate(() => runFullFlowJob(job.id));
  }

  const fromEventIndex = Number(req.query.fromEventIndex || 0);
  const events = (job.events || []).filter((event) => event.eventIndex >= fromEventIndex);
  const nextEventIndex = events.length ? events[events.length - 1].eventIndex + 1 : fromEventIndex;
  return res.json({ ok: true, job: publicJob(job), events, nextEventIndex });
});

app.post('/flow/pause-full-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job) return res.status(404).json({ ok: false, error: 'Job not found' });
  if (!['done', 'failed', 'cancelled'].includes(job.status)) {
    job.status = 'paused';
    job.pauseRequested = true;
    pushJobEvent(job, { type: 'section', message: '⏸ Đã tạm dừng job. Các request đang chạy sẽ hoàn tất rồi đứng lại.' });
  }
  return res.json({ ok: true, job: publicJob(job) });
});

app.post('/flow/resume-full-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job) return res.status(404).json({ ok: false, error: 'Job not found' });
  if (['done', 'failed', 'cancelled'].includes(job.status)) {
    return res.status(400).json({ ok: false, error: `Job đã ${job.status}, không resume được.` });
  }
  for (const item of job.items || []) {
    if (item.status === 'processing') item.status = 'pending';
  }
  job.status = 'running';
  job.pauseRequested = false;
  job.cancelRequested = false;
  job.rateLimit = job.rateLimit ? { ...job.rateLimit, active: false } : null;
  pushJobEvent(job, { type: 'section', message: '▶ Resume job. Tool chạy tiếp từ ID chưa xong, không chạy lại ID success.' });
  persistJob(job);
  setImmediate(() => runFullFlowJob(job.id));
  return res.json({ ok: true, job: publicJob(job) });
});

app.post('/flow/cancel-full-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job) return res.status(404).json({ ok: false, error: 'Job not found' });
  job.cancelRequested = true;
  job.status = 'cancelled';
  for (const item of job.items || []) {
    if (item.status === 'processing') item.status = 'pending';
  }
  pushJobEvent(job, { type: 'section', message: '🛑 Đã yêu cầu dừng hẳn job. Các dòng success vẫn được lưu resume.' });
  persistJob(job);
  return res.json({ ok: true, job: publicJob(job) });
});

async function deleteCampaignWithVerify({ job, campaignId }) {
  const result = await deleteCampaign({ campaignId });
  let verify = null;
  try {
    verify = await getCampaign(campaignId);
  } catch (err) {
    const msg = String(err?.message || '').toLowerCase();
    if (isRateLimitJobError(err)) throw err;
    if (msg.includes('does not exist') || msg.includes('cannot be loaded') || msg.includes('unsupported delete request')) {
      return { ...result, verified: true, verifyStatus: 'not_loadable_after_delete' };
    }
    return { ...result, verified: false, verifyError: err.message || 'verify failed' };
  }
  return {
    ...result,
    verified: verify?.effective_status === 'DELETED' || verify?.status === 'DELETED',
    verify
  };
}

async function processDeleteItemWithSmartRetry({ job, item, workerId }) {
  for (let attempt = Number(item.attempts || 0); attempt <= job.maxRetries; attempt += 1) {
    item.attempts = attempt;
    try {
      return await deleteCampaignWithVerify({ job, campaignId: item.campaignId });
    } catch (err) {
      item.lastError = err.message || 'Lỗi xóa campaign';
      item.errorType = classifyJobError(err);
      if (isRateLimitJobError(err)) {
        markGlobalRateLimit(job, err, { workerId, item });
        return { rateLimited: true };
      }
      const retryable = isRetryableNetworkJobError(err);
      if (!retryable || attempt >= job.maxRetries) throw err;
      const waitMs = retryDelayMs(attempt, job.delayMs);
      pushJobEvent(job, {
        type: 'running',
        workerId,
        campaignId: item.campaignId,
        message: `[Bàn ${workerId}] Campaign ${item.campaignId} lỗi tạm thời, retry lần ${attempt + 1}/${job.maxRetries} sau ${Math.round(waitMs / 1000)}s: ${err.message || 'lỗi API'}`
      });
      await sleep(waitMs);
    }
  }
  throw new Error('Unknown delete campaign error');
}

async function runDeleteCampaignJob(jobId) {
  const job = getJobAny(jobId);
  if (!job || job.runnerActive) return;
  job.runnerActive = true;
  job.status = job.status === 'queued' ? 'running' : job.status;
  job.startedAt = job.startedAt || new Date().toISOString();
  pushJobEvent(job, { type: 'section', message: `Job xóa campaign bắt đầu/tiếp tục: ${job.total} campaign | ${job.concurrency} bàn | delay ${job.delayMs}ms | retry ${job.maxRetries}` });

  async function worker(workerId) {
    while (true) {
      if (!(await waitForManualPauseOrRateLimit(job, workerId))) return;
      if (job.cancelRequested || job.status === 'cancelled' || job.status !== 'running') return;
      const item = (job.items || []).find((x) => x.status === 'pending');
      if (!item) return;
      item.status = 'processing';
      item.workerId = workerId;
      if (job.delayMs > 0 && item.index > 0) await sleep(job.delayMs);
      pushJobEvent(job, { type: 'running', index: item.index, workerId, campaignId: item.campaignId, message: `[Bàn ${workerId}] Nhận campaign ${item.index + 1}/${job.total}: ${item.campaignId}` });
      try {
        const result = await processDeleteItemWithSmartRetry({ job, item, workerId });
        if (result?.rateLimited) continue;
        item.status = 'success';
        item.result = result;
        item.finishedAt = new Date().toISOString();
        job.completed += 1;
        job.success += 1;
        job.results[item.index] = { ok: true, index: item.index, campaignId: item.campaignId, result };
        pushJobEvent(job, { type: 'success', index: item.index, campaignId: item.campaignId, result, message: `[Bàn ${workerId}] ${item.campaignId} - Đã xóa bằng API${result.verified ? ' + verify OK' : ''}` });
      } catch (err) {
        item.status = 'failed';
        item.error = err.message || 'Lỗi backend';
        item.errorType = classifyJobError(err);
        item.finishedAt = new Date().toISOString();
        job.completed += 1;
        job.failed += 1;
        job.results[item.index] = { ok: false, index: item.index, campaignId: item.campaignId, error: item.error, errorType: item.errorType, meta: err.meta || null };
        pushJobEvent(job, { type: 'error', index: item.index, campaignId: item.campaignId, error: item.error, errorType: item.errorType, message: `[Bàn ${workerId}] ${item.campaignId} - Xóa lỗi: ${item.error}${item.errorType ? ` [${item.errorType}]` : ''}` });
      }
      persistJob(job);
    }
  }

  try {
    const workers = Array.from({ length: Math.min(job.concurrency, job.items.filter((x) => x.status === 'pending').length || 1) }, (_, i) => worker(i + 1));
    await Promise.all(workers);
    if (job.cancelRequested || job.status === 'cancelled') {
      job.status = 'cancelled';
      job.finishedAt = new Date().toISOString();
      pushJobEvent(job, { type: 'section', message: `Job xóa đã dừng: SUCCESS ${job.success} | FAILED ${job.failed} | TOTAL ${job.total}` });
    } else if (job.status === 'paused' || job.status === 'rate_limited') {
      pushJobEvent(job, { type: 'section', message: `Job xóa đang ${job.status}. Có thể resume tiếp.` });
    } else if (!hasPendingItems(job)) {
      job.status = 'done';
      job.finishedAt = new Date().toISOString();
      pushJobEvent(job, { type: 'section', message: `Job xóa campaign kết thúc: SUCCESS ${job.success} | FAILED ${job.failed} | TOTAL ${job.total}` });
    }
  } finally {
    job.runnerActive = false;
    persistJob(job);
  }
}

app.post('/campaigns/start-delete-job', async (req, res) => {
  try {
    const { campaignIds, settings = {} } = req.body || {};
    if (!Array.isArray(campaignIds) || !campaignIds.length) return res.status(400).json({ ok: false, error: 'Missing campaignIds' });
    if (campaignIds.length > 5000) return res.status(400).json({ ok: false, error: 'Quá nhiều campaign trong một job. Chia nhỏ tối đa 5000 campaign/job.' });

    const cleanCampaignIds = [...new Set(campaignIds.map((id) => String(id || '').trim().replace(/^act_/, '')).filter((id) => /^\d{8,}$/.test(id)))];
    if (!cleanCampaignIds.length) return res.status(400).json({ ok: false, error: 'Không có campaign_id hợp lệ để xóa.' });

    const concurrency = clampNumber(settings.concurrency ?? process.env.DELETE_CAMPAIGN_CONCURRENCY, 4, 1, 8);
    const delayMs = clampNumber(settings.delayMs ?? process.env.DELETE_CAMPAIGN_DELAY_MS, 3000, 0, 300000);
    const maxRetries = clampNumber(settings.maxRetries ?? process.env.DELETE_CAMPAIGN_MAX_RETRIES, 2, 0, 10);
    const rateLimitCooldownMs = clampNumber(settings.rateLimitCooldownMs ?? process.env.RATE_LIMIT_COOLDOWN_MS, DEFAULT_RATE_LIMIT_COOLDOWN_MS, 60 * 1000, 2 * 60 * 60 * 1000);

    const id = makeJobId();
    const job = {
      id,
      kind: 'delete_campaigns',
      status: 'queued',
      campaignIds: cleanCampaignIds,
      items: cleanCampaignIds.map((campaignId, index) => ({ index, campaignId, status: 'pending', attempts: 0 })),
      total: cleanCampaignIds.length,
      completed: 0,
      success: 0,
      failed: 0,
      skipped: 0,
      concurrency,
      delayMs,
      maxRetries,
      rateLimitCooldownMs,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      startedAt: null,
      finishedAt: null,
      cancelRequested: false,
      pauseRequested: false,
      autoResume: true,
      runnerActive: false,
      error: null,
      rateLimit: null,
      results: new Array(cleanCampaignIds.length),
      events: [],
      nextEventIndex: 0
    };
    persistJob(job);
    setImmediate(() => runDeleteCampaignJob(id));
    return res.json({ ok: true, job: publicJob(job) });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message || 'Cannot start delete campaign job', errorType: classifyJobError(err), meta: err.meta || null });
  }
});

app.get('/campaigns/delete-job-status/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job || job.kind !== 'delete_campaigns') return res.status(404).json({ ok: false, error: 'Delete campaign job not found' });
  if ((job.status === 'queued' || job.status === 'rate_limited' || job.status === 'running') && !job.runnerActive && hasPendingItems(job)) setImmediate(() => runDeleteCampaignJob(job.id));
  const fromEventIndex = Number(req.query.fromEventIndex || 0);
  const events = (job.events || []).filter((event) => event.eventIndex >= fromEventIndex);
  const nextEventIndex = events.length ? events[events.length - 1].eventIndex + 1 : fromEventIndex;
  return res.json({ ok: true, job: publicJob(job), events, nextEventIndex });
});

app.post('/campaigns/pause-delete-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job || job.kind !== 'delete_campaigns') return res.status(404).json({ ok: false, error: 'Delete campaign job not found' });
  if (!['done', 'failed', 'cancelled'].includes(job.status)) {
    job.status = 'paused';
    pushJobEvent(job, { type: 'section', message: '⏸ Đã tạm dừng job xóa.' });
  }
  return res.json({ ok: true, job: publicJob(job) });
});

app.post('/campaigns/resume-delete-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job || job.kind !== 'delete_campaigns') return res.status(404).json({ ok: false, error: 'Delete campaign job not found' });
  for (const item of job.items || []) if (item.status === 'processing') item.status = 'pending';
  job.status = 'running';
  job.cancelRequested = false;
  job.rateLimit = job.rateLimit ? { ...job.rateLimit, active: false } : null;
  pushJobEvent(job, { type: 'section', message: '▶ Resume job xóa campaign.' });
  persistJob(job);
  setImmediate(() => runDeleteCampaignJob(job.id));
  return res.json({ ok: true, job: publicJob(job) });
});

app.post('/campaigns/cancel-delete-job/:jobId', (req, res) => {
  const job = getJobAny(req.params.jobId);
  if (!job || job.kind !== 'delete_campaigns') return res.status(404).json({ ok: false, error: 'Delete campaign job not found' });
  job.cancelRequested = true;
  job.status = 'cancelled';
  for (const item of job.items || []) if (item.status === 'processing') item.status = 'pending';
  pushJobEvent(job, { type: 'section', message: '🛑 Đã yêu cầu dừng job xóa. Các request đang chạy sẽ hoàn tất rồi dừng.' });
  persistJob(job);
  return res.json({ ok: true, job: publicJob(job) });
});

app.post('/adsets/create-draft', async (req, res) => {
  try {
    const { adAccountId, campaignId, adSetName, pageId, optimizationGoal } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!campaignId) return res.status(400).json({ error: 'Missing campaignId' });
    if (!adSetName) return res.status(400).json({ error: 'Missing adSetName' });
    if (!pageId) return res.status(400).json({ error: 'Missing pageId' });

    const adSet = await createAdSetDraft({
      adAccountId,
      campaignId,
      adSetName,
      pageId,
      optimizationGoal
    });
    const detail = await getAdSet(adSet.id);

    const adsManagerUrl = buildAdsManagerUrl({
      adAccountId,
      campaignId,
      adSetId: adSet.id
    });

    res.json({
      ok: true,
      adSet: {
        id: detail.id,
        name: detail.name,
        status: detail.status,
        campaign_id: detail.campaign_id,
        daily_budget: detail.daily_budget,
        optimization_goal: detail.optimization_goal,
        destination_type: detail.destination_type
      },
      adsManagerUrl
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/jobs/create-adsets', async (_req, res) => {
  try {
    const jobs = readJobs();

    if (!jobs.length) {
      return res.status(400).json({ error: 'No jobs found' });
    }

    const results = [];
    const nextJobs = [...jobs];

    for (let i = 0; i < nextJobs.length; i++) {
      const job = nextJobs[i];

      if (!job.campaignId) {
        results.push({
          ok: false,
          pageId: job.pageId,
          errorType: 'missing_campaign_id',
          error: 'Missing campaignId'
        });
        continue;
      }

      if (!job.pageId) {
        results.push({
          ok: false,
          campaignId: job.campaignId,
          errorType: 'missing_page_id',
          error: 'Missing pageId'
        });
        continue;
      }

      if (!job.budget || Number(job.budget) <= 0) {
        results.push({
          ok: false,
          pageId: job.pageId,
          campaignId: job.campaignId,
          errorType: 'invalid_budget',
          error: 'Invalid budget'
        });
        continue;
      }

      try {
        const adSetName = `Nhóm QC - ${job.pageId}`;

        const created = await createAdSetDraft({
          adAccountId: job.adAccountId,
          campaignId: job.campaignId,
          adSetName,
          pageId: job.pageId
        });
        const detail = await getAdSet(created.id);

        nextJobs[i] = {
          ...job,
          adSetId: detail.id,
          adSetName: detail.name,
          adSetStatus: detail.status,
          optimizationGoal: detail.optimization_goal,
          destinationType: detail.destination_type,
          status: 'adset_created',
          adSetCreatedAt: new Date().toISOString()
        };

        results.push({
          ok: true,
          pageId: job.pageId,
          budget: job.budget,
          campaignId: job.campaignId,
          adSetId: detail.id,
          adSetName: detail.name,
          adSetStatus: detail.status
        });
      } catch (err) {
        const message = err.message || 'Unknown error';
        const lower = message.toLowerCase();

        let errorType = 'unknown';

        if (
          lower.includes('không đủ quyền đối với trang') ||
          lower.includes('khong du quyen doi voi trang') ||
          lower.includes('not authorized to use page') ||
          (lower.includes('permission') && lower.includes('page'))
        ) {
          errorType = 'no_page_permission';
        } else if (
          lower.includes('must be a valid page id') ||
          lower.includes('invalid page') ||
          lower.includes('promoted_object[page_id]')
        ) {
          errorType = 'invalid_page_id';
        }

        nextJobs[i] = {
          ...job,
          status: 'adset_failed',
          adSetError: message,
          adSetErrorType: errorType,
          adSetFailedAt: new Date().toISOString()
        };

        results.push({
          ok: false,
          pageId: job.pageId,
          budget: job.budget,
          campaignId: job.campaignId,
          errorType,
          error: message
        });
      }
    }

    writeJobs(nextJobs);

    res.json({
      ok: true,
      successCount: results.filter((x) => x.ok).length,
      failCount: results.filter((x) => !x.ok).length,
      results
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/campaigns/create-draft', async (req, res) => {
  try {
    const {
      adAccountId,
      campaignName,
      objective,
      dailyBudget,
      pageRef,
      savedAudienceName,
      postId
    } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!campaignName) return res.status(400).json({ error: 'Missing campaignName' });

    const campaign = await createCampaignDraft({
      adAccountId,
      campaignName,
      objective,
      dailyBudget
    });

    const adsManagerUrl = buildAdsManagerUrl({
      adAccountId,
      campaignId: campaign.id
    });

    res.json({
      ok: true,
      inputEcho: {
        dailyBudget,
        pageRef,
        savedAudienceName,
        postId
      },
      campaign: {
        id: campaign.id,
        name: campaignName
      },
      adsManagerUrl
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/campaigns/create-draft-batch', async (req, res) => {
  try {
    const { adAccountId, campaignNameTemplate, objective, items } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!Array.isArray(items) || !items.length) {
      return res.status(400).json({ error: 'Missing items' });
    }

    const results = [];
    const failedPageIds = [];
    const jobsToSave = [];
    const numericAccountId = adAccountId.replace(/^act_/, '');

    for (const rawItem of items) {
      const pageId = String(rawItem?.pageId || '').trim();
      const budget = Number(rawItem?.budget);

      if (!pageId) {
        results.push({
          ok: false,
          pageId: '',
          budget: rawItem?.budget,
          error: 'Missing pageId'
        });
        continue;
      }

      if (!Number.isFinite(budget) || budget <= 0) {
        failedPageIds.push(pageId);
        results.push({
          ok: false,
          pageId,
          budget: rawItem?.budget,
          error: 'Invalid budget'
        });
        continue;
      }

      const campaignName = (campaignNameTemplate || 'AUTO {{pageId}}').replaceAll('{{pageId}}', pageId);

      try {
        const campaign = await createCampaignDraft({
          adAccountId,
          campaignName,
          objective,
          dailyBudget: budget
        });

        const adsManagerUrl =
          `https://adsmanager.facebook.com/adsmanager/manage/campaigns?act=${numericAccountId}` +
          `&selected_campaign_ids=${campaign.id}`;

        const jobRecord = {
          pageId,
          budget,
          adAccountId,
          objective: objective || 'OUTCOME_ENGAGEMENT',
          campaignId: campaign.id,
          campaignName,
          adsManagerUrl,
          status: 'campaign_created',
          createdAt: new Date().toISOString()
        };

        jobsToSave.push(jobRecord);

        results.push({
          ok: true,
          ...jobRecord
        });
      } catch (err) {
        failedPageIds.push(pageId);
        results.push({
          ok: false,
          pageId,
          budget,
          error: err.message
        });
      }
    }

    appendJobs(jobsToSave);

    res.json({
      ok: true,
      successCount: results.filter((x) => x.ok).length,
      failCount: results.filter((x) => !x.ok).length,
      failedPageIds,
      savedCount: jobsToSave.length,
      results
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/jobs', (_req, res) => {
  try {
    const jobs = readJobs();
    res.json({
      ok: true,
      count: jobs.length,
      jobs
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get('/jobs/adsets', (_req, res) => {
  try {
    const jobs = readJobs().filter((job) => job.adSetId);
    res.json({
      ok: true,
      count: jobs.length,
      jobs
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.post('/ads/create-draft', async (req, res) => {
  try {
    const { adAccountId, adSetId, adName, postId } = req.body;

    if (!adAccountId) return res.status(400).json({ error: 'Missing adAccountId' });
    if (!adSetId) return res.status(400).json({ error: 'Missing adSetId' });
    if (!adName) return res.status(400).json({ error: 'Missing adName' });
    if (!postId) return res.status(400).json({ error: 'Missing postId' });

    const ad = await createAdDraft({
      adAccountId,
      adSetId,
      adName,
      postId
    });

    const detail = await getAd(ad.id);
    const adsManagerUrl = buildAdsManagerUrl({
      adAccountId,
      adSetId,
      adId: ad.id
    });

    res.json({
      ok: true,
      ad: {
        id: detail.id,
        name: detail.name,
        status: detail.status,
        adset_id: detail.adset_id,
        campaign_id: detail.campaign_id
      },
      adsManagerUrl
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});
async function exchangeCodeForUserToken(code) {
  if (!FB_APP_ID) throw new Error('Missing META_APP_ID');
  if (!FB_APP_SECRET) throw new Error('Missing META_APP_SECRET');
  if (!FB_REDIRECT_URI) throw new Error('Missing META_REDIRECT_URI');

  const url =
    `https://graph.facebook.com/v23.0/oauth/access_token` +
    `?client_id=${encodeURIComponent(FB_APP_ID)}` +
    `&redirect_uri=${encodeURIComponent(FB_REDIRECT_URI)}` +
    `&client_secret=${encodeURIComponent(FB_APP_SECRET)}` +
    `&code=${encodeURIComponent(code)}`;

  const res = await fetch(url);
  const data = await res.json();

  if (!res.ok || data.error) {
    throw new Error(
      data?.error?.message || 'Không đổi được code sang user token'
    );
  }

  return data;
}

async function exchangeLongLivedUserToken(shortLivedUserToken) {
  if (!FB_APP_ID) throw new Error('Missing META_APP_ID');
  if (!FB_APP_SECRET) throw new Error('Missing META_APP_SECRET');

  const url =
    `https://graph.facebook.com/v23.0/oauth/access_token` +
    `?grant_type=fb_exchange_token` +
    `&client_id=${encodeURIComponent(FB_APP_ID)}` +
    `&client_secret=${encodeURIComponent(FB_APP_SECRET)}` +
    `&fb_exchange_token=${encodeURIComponent(shortLivedUserToken)}`;

  const res = await fetch(url);
  const data = await res.json();

  if (!res.ok || data.error) {
    throw new Error(
      data?.error?.message || 'Không đổi được long-lived user token'
    );
  }

  return data;
}


app.get('/auth/permissions', async (_req, res) => {
  try {
    const store = readTokenStore();

    if (!store?.userToken) {
      return res.status(400).json({
        ok: false,
        error: 'No token'
      });
    }

    const url =
      `https://graph.facebook.com/v23.0/me/permissions` +
      `?access_token=${encodeURIComponent(store.userToken)}`;

    const fbRes = await fetch(url);
    const data = await fbRes.json();

    return res.status(fbRes.ok ? 200 : 400).json(data);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message || 'Cannot read permissions'
    });
  }
});

app.get('/auth/facebook/start', (_req, res) => {
  if (!FB_APP_ID) {
    return res.status(400).send('Missing META_APP_ID');
  }

  const scope = [
    'ads_management',
    'ads_read',
    'pages_show_list',
    'pages_read_engagement',
    'business_management'
  
  ].join(',');

  const authUrl =
  `https://www.facebook.com/v23.0/dialog/oauth` +
  `?client_id=${encodeURIComponent(FB_APP_ID)}` +
  `&redirect_uri=${encodeURIComponent(FB_REDIRECT_URI)}` +
  `&scope=${encodeURIComponent(scope)}` +
  `&auth_type=rerequest`;

  res.redirect(authUrl);
});

app.get('/auth/facebook/callback', async (req, res) => {
  try {
    const errorMessage = String(req.query.error_message || req.query.error || '');
    const errorCode = String(req.query.error_code || '');

    if (errorMessage) {
      return res.status(400).send(
        `Facebook login error${errorCode ? ` (${errorCode})` : ''}: ${errorMessage}`
      );
    }

    const code = String(req.query.code || '');
    if (!code) {
      return res.status(400).send('Missing code');
    }

    const shortToken = await exchangeCodeForUserToken(code);
    const longToken = await exchangeLongLivedUserToken(shortToken.access_token);
    const me = await getFacebookMe(longToken.access_token);

    saveUserToken({
      userToken: longToken.access_token,
      tokenType: 'user',
      expiresIn: longToken.expires_in || null,
      meta: {
        source: 'oauth_callback',
        facebookUserId: me.id || null,
        facebookUserName: me.name || null
      }
    });

    res.send(`
      <!doctype html>
      <html lang="vi">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>Kết nối Facebook thành công</title>
        </head>
        <body style="font-family:Arial,sans-serif;padding:40px;text-align:center;">
          <h1>Kết nối Facebook thành công</h1>
          <p>Đã lưu token cho tài khoản: <strong>${me.name || 'Không rõ tên'}</strong></p>
          <p>Facebook ID: <strong>${me.id || 'Không rõ ID'}</strong></p>
          <p>Bạn có thể đóng tab này và quay lại ứng dụng.</p>
        </body>
      </html>
    `);
  } catch (err) {
    res.status(400).send(`Auth callback error: ${err.message}`);
  }
});
async function getFacebookMe(userToken) {
  const url =
    `https://graph.facebook.com/v23.0/me` +
    `?fields=id,name` +
    `&access_token=${encodeURIComponent(userToken)}`;

  const res = await fetch(url);
  const data = await res.json();

  if (!res.ok || data.error) {
    throw new Error(data?.error?.message || 'Không lấy được thông tin tài khoản Facebook');
  }

  return data;
}

app.get('/auth/status', (_req, res) => {
  const store = readTokenStore();

  res.json({
    ok: true,
    hasToken: !!store?.userToken,
    connected: !!store?.userToken,
    tokenType: store?.tokenType || null,
    expiresAt: store?.expiresAt || null,
    updatedAt: store?.updatedAt || null,
    profile: store?.userToken
      ? {
          id: store?.meta?.facebookUserId || null,
          name: store?.meta?.facebookUserName || null
        }
      : null,
    meta: store?.meta || {}
  });
});

app.post('/auth/logout', (_req, res) => {
  clearStoredUserToken();
  res.json({ ok: true });
});

const PORT = process.env.PORT || 8080;

await initUltraStore();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend listening on port ${PORT}`);
});