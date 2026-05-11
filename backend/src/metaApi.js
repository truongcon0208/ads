import { getStoredUserToken } from './tokenStore.js';
function getCurrentUserToken() {
  return getStoredUserToken() || process.env.META_USER_ACCESS_TOKEN || process.env.META_ACCESS_TOKEN || null;
}
const API_VERSION = process.env.META_API_VERSION || 'v23.0';
const BASE_URL = `https://graph.facebook.com/${API_VERSION}`;


const DEFAULT_META_MAX_RETRIES = Number(process.env.META_API_MAX_RETRIES || 4);
const DEFAULT_META_BASE_DELAY_MS = Number(process.env.META_API_BASE_DELAY_MS || 4000);
const DEFAULT_META_MAX_DELAY_MS = Number(process.env.META_API_MAX_DELAY_MS || 60000);
const DEFAULT_META_REQUEST_DELAY_MS = Number(process.env.META_API_REQUEST_DELAY_MS || 300);
const DEFAULT_META_JITTER_MS = Number(process.env.META_API_JITTER_MS || 1000);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampNumber(value, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function getRetryDelayMs(attempt) {
  const baseDelay = clampNumber(DEFAULT_META_BASE_DELAY_MS, 4000, 0, 300000);
  const maxDelay = clampNumber(DEFAULT_META_MAX_DELAY_MS, 60000, 1000, 600000);
  const jitter = clampNumber(DEFAULT_META_JITTER_MS, 1000, 0, 10000);
  const delay = Math.min(maxDelay, baseDelay * Math.pow(2, attempt));
  return delay + Math.floor(Math.random() * jitter);
}

function isRateLimitMetaError(meta = {}, message = '') {
  const code = Number(meta?.code);
  const subcode = Number(meta?.error_subcode || meta?.subcode);
  const lower = String(message || meta?.message || '').toLowerCase();

  return (
    [4, 17, 32, 613, 80004].includes(code) ||
    [2446079, 1487390].includes(subcode) ||
    lower.includes('rate limit') ||
    lower.includes('too many calls') ||
    lower.includes('temporarily blocked') ||
    lower.includes('please reduce the amount of data') ||
    lower.includes('try again later')
  );
}

function isRetriableFetchError(err) {
  const lower = String(err?.message || '').toLowerCase();
  return (
    err?.retryable === true ||
    err?.errorType === 'RATE_LIMIT' ||
    lower.includes('fetch failed') ||
    lower.includes('network') ||
    lower.includes('timeout') ||
    lower.includes('econnreset') ||
    lower.includes('etimedout') ||
    lower.includes('socket')
  );
}

function makeMetaError(data, fallbackMessage = 'Meta API request failed') {
  const meta = data?.error || null;
  const message =
    meta?.error_user_title ||
    meta?.error_user_msg ||
    meta?.message ||
    fallbackMessage;

  const err = new Error(message);
  err.meta = meta;
  err.errorType = isRateLimitMetaError(meta, message) ? 'RATE_LIMIT' : classifyMetaError(message);
  err.retryable = err.errorType === 'RATE_LIMIT';
  return err;
}

async function parseJsonResponse(response) {
  const raw = await response.text();
  if (!raw) return {};

  try {
    return JSON.parse(raw);
  } catch {
    const err = new Error(`Meta API returned non-JSON response. HTTP ${response.status}`);
    err.status = response.status;
    err.retryable = response.status >= 500 || response.status === 429;
    err.errorType = err.retryable ? 'RATE_LIMIT' : 'UNKNOWN';
    throw err;
  }
}

async function graphFetch(path, { options = {}, fallbackMessage = 'Meta API request failed', accessTokenRequired = true } = {}) {
  const token = getCurrentUserToken();

  if (accessTokenRequired && !token) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  const isForm = options.body instanceof URLSearchParams;
  const headers = {
    ...(isForm ? {} : { 'Content-Type': 'application/json' }),
    ...(options.headers || {})
  };

  const maxRetries = clampNumber(
    options.maxRetries ?? DEFAULT_META_MAX_RETRIES,
    DEFAULT_META_MAX_RETRIES,
    0,
    10
  );
  const requestDelayMs = clampNumber(DEFAULT_META_REQUEST_DELAY_MS, 300, 0, 60000);

  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (requestDelayMs > 0) {
      await sleep(requestDelayMs);
    }

    try {
      const response = await fetch(`${BASE_URL}${path}`, {
        method: options.method || 'GET',
        headers,
        body: options.body
      });

      const data = await parseJsonResponse(response);

      if (!response.ok || data.error) {
        const err = makeMetaError(data, fallbackMessage);
        err.status = response.status;
        err.attempt = attempt + 1;

        // HTTP 429/5xx có thể là nghẽn/rate limit ngay cả khi Meta không trả code rõ ràng.
        if (response.status === 429 || response.status >= 500) {
          err.retryable = true;
          if (err.errorType === 'UNKNOWN') err.errorType = 'RATE_LIMIT';
        }

        throw err;
      }

      return data;
    } catch (err) {
      lastError = err;

      // Với rate limit thật (#4/613/80004...), đẩy lỗi ra job manager ngay
      // để pause toàn job theo phút. Không retry âm thầm ở tầng Meta API.
      if (err?.errorType === 'RATE_LIMIT') {
        throw err;
      }

      const retryable = isRetriableFetchError(err);

      if (!retryable || attempt >= maxRetries) {
        throw err;
      }

      await sleep(getRetryDelayMs(attempt));
    }
  }

  throw lastError || new Error(fallbackMessage);
}


export function normalizeAdAccountId(adAccountId) {
  const raw = String(adAccountId || '').trim();
  if (!raw) return '';
  return raw.startsWith('act_') ? raw : `act_${raw}`;
}

export function normalizeNumericId(id) {
  return String(id || '').trim().replace(/^act_/, '');
}

export function classifyMetaError(message = '') {
  const lower = String(message || '').toLowerCase();

  if (
    lower.includes('rate limit') ||
    lower.includes('too many calls') ||
    lower.includes('temporarily blocked') ||
    lower.includes('try again later')
  ) {
    return 'RATE_LIMIT';
  }

  if (
    lower.includes('not allowed for this call') ||
    lower.includes('permission') ||
    lower.includes('not authorized') ||
    lower.includes('does not have permission') ||
    lower.includes('requires business_management')
  ) {
    return 'NO_PERMISSION';
  }

  if (lower.includes('invalid') && lower.includes('page')) return 'INVALID_PAGE_ID';
  if (lower.includes('invalid') && lower.includes('account')) return 'INVALID_AD_ACCOUNT_ID';
  if (lower.includes('disabled') || lower.includes('account_status')) return 'AD_ACCOUNT_DISABLED';

  return 'UNKNOWN';
}

async function metaFetch(path, options = {}) {
  return graphFetch(path, {
    options,
    fallbackMessage: 'Meta API request failed',
    accessTokenRequired: true
  });
}

async function fetchAllPages(firstPath) {
  const token = getCurrentUserToken();
  let path = firstPath;
  const all = [];

  while (path) {
    const data = await metaFetch(path);
    if (Array.isArray(data?.data)) all.push(...data.data);

    const next = data?.paging?.next;
    if (!next) break;

    const url = new URL(next);
    path = `${url.pathname.replace(`/${API_VERSION}`, '')}${url.search}`;

    // Safety limit so a bad paging response cannot loop forever.
    if (all.length > 5000) break;
  }

  return all;
}

export async function listMyAdAccounts() {
  const token = getCurrentUserToken();
  if (!token) throw new Error('Missing user token. Please connect Facebook again.');

  return fetchAllPages(
    `/me/adaccounts?fields=id,account_id,name,account_status&limit=100&access_token=${encodeURIComponent(token)}`
  );
}

export async function listMyPages() {
  const token = getCurrentUserToken();
  if (!token) throw new Error('Missing user token. Please connect Facebook again.');

  return fetchAllPages(
    `/me/accounts?fields=id,name&limit=100&access_token=${encodeURIComponent(token)}`
  );
}

export async function scanPermissions({ adAccountId, pageIds = [] }) {
  const normalizedAdAccountId = normalizeAdAccountId(adAccountId);
  const uniquePageIds = [...new Set(
    (pageIds || []).map((x) => String(x || '').trim()).filter(Boolean)
  )];

  let adAccount = null;
  let adAccountOk = false;
  let adAccountReason = null;

  try {
    adAccount = await getAdAccount(normalizedAdAccountId);
    adAccountOk = Boolean(adAccount?.id);
  } catch (err) {
    adAccountReason = err.errorType || 'NO_AD_ACCOUNT_PERMISSION';
  }

  const pageResults = [];

  for (const pageId of uniquePageIds) {
    try {
      const page = await metaFetch(
        `/${pageId}?fields=id,name&access_token=${encodeURIComponent(getCurrentUserToken())}`
      );

      pageResults.push({
        pageId,
        ok: Boolean(page?.id),
        name: page?.name || null,
        tasks: [],
        reason: page?.id ? null : 'NO_PAGE_PERMISSION'
      });
    } catch (err) {
      const msg = err?.message || '';
    
      let errorType = 'UNKNOWN';
    
      if (msg.includes('global id') || msg.includes('not allowed for this call')) {
        errorType = 'GLOBAL_ID_NOT_ALLOWED_FOR_OWNED_PAGES';
      } else if (msg.includes('Permissions error') || msg.includes('permission')) {
        errorType = 'PERMISSION_ERROR';
      } else if (msg.includes('already owned')) {
        errorType = 'ALREADY_OWNED_BY_ANOTHER_BUSINESS';
      }
    
      pageResults.push({
        pageId,
        ok: false,
        name: null,
        tasks: [],
        reason: errorType,
        error: msg,
        meta: err.meta || null
      });
    }
  }

  return {
    ok: adAccountOk && pageResults.every((x) => x.ok),
    adAccount: {
      input: adAccountId,
      normalized: normalizedAdAccountId,
      ok: adAccountOk,
      id: adAccount?.id || null,
      account_id: adAccount?.account_id || null,
      name: adAccount?.name || null,
      account_status: adAccount?.account_status || null,
      reason: adAccountOk ? null : adAccountReason
    },
    pages: pageResults,
    summary: {
      totalPages: pageResults.length,
      allowedPages: pageResults.filter((x) => x.ok).length,
      blockedPages: pageResults.filter((x) => !x.ok).length
    }
  };
}

export async function getAdAccount(adAccountId) {
  const normalized = normalizeAdAccountId(adAccountId);
  return metaFetch(
    `/${normalized}?fields=id,name,account_id,account_status&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function listCampaigns(adAccountId) {
  const normalized = normalizeAdAccountId(adAccountId);
  return metaFetch(
    `/${normalized}/campaigns?fields=id,name,status,effective_status,created_time,updated_time&limit=500&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function listAllCampaigns(adAccountId) {
  const normalized = normalizeAdAccountId(adAccountId);
  return fetchAllPages(
    `/${normalized}/campaigns?fields=id,name,status,effective_status,created_time,updated_time&limit=500&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function getCampaign(campaignId) {
  const cleanCampaignId = normalizeNumericId(campaignId);
  return metaFetch(
    `/${cleanCampaignId}?fields=id,name,status,effective_status&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function createCampaignDraft({
  adAccountId,
  campaignName,
  objective,
  dailyBudget
}) {
  const normalized = normalizeAdAccountId(adAccountId);
  const body = new URLSearchParams({
    name: campaignName,
    objective: objective || 'OUTCOME_ENGAGEMENT',
    status: 'PAUSED',
    special_ad_categories: '[]',
    daily_budget: String(dailyBudget),
    bid_strategy: 'LOWEST_COST_WITHOUT_CAP',
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${normalized}/campaigns`, {
    method: 'POST',
    body
  });
}

export async function createAdSetDraft({
  adAccountId,
  campaignId,
  adSetName,
  pageId,
  optimizationGoal = 'CONVERSATIONS',
  billingEvent = 'IMPRESSIONS'
}) {
  const normalized = normalizeAdAccountId(adAccountId);
  const body = new URLSearchParams({
    name: adSetName,
    campaign_id: campaignId,
    billing_event: billingEvent,
    optimization_goal: optimizationGoal,
    status: 'PAUSED',
    destination_type: 'MESSENGER',
    promoted_object: JSON.stringify({
      page_id: pageId
    }),
    targeting: JSON.stringify({
      geo_locations: {
        custom_locations: [
          {
            latitude: 13.695484180036347,
            longitude: 108.08100700378418,
            radius: 4,
            distance_unit: 'kilometer'
          }
        ]
      },
      publisher_platforms: ['messenger', 'facebook'],
      facebook_positions: ['feed'],
      messenger_positions: ['story']
    }),
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${normalized}/adsets`, {
    method: 'POST',
    body
  });
}

export async function createAdDraft({
  adAccountId,
  adSetId,
  adName,
  postId
}) {
  const normalized = normalizeAdAccountId(adAccountId);
  const body = new URLSearchParams({
    name: adName,
    adset_id: adSetId,
    status: 'PAUSED',
    creative: JSON.stringify({
      object_story_id: postId
    }),
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${normalized}/ads`, {
    method: 'POST',
    body
  });
}

export async function getAd(adId) {
  return metaFetch(
    `/${adId}?fields=id,name,status,adset_id,campaign_id,creative&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function getAdSet(adSetId) {
  return metaFetch(
    `/${adSetId}?fields=id,name,status,campaign_id,daily_budget,optimization_goal,destination_type&access_token=${encodeURIComponent(getCurrentUserToken())}`
  );
}

export async function createAdDraftWithObjectStoryId({
  adAccountId,
  adSetId,
  adName,
  objectStoryId
}) {
  const normalized = normalizeAdAccountId(adAccountId);
  const body = new URLSearchParams({
    name: adName,
    adset_id: adSetId,
    status: 'PAUSED',
    creative: JSON.stringify({
      object_story_id: objectStoryId
    }),
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${normalized}/ads`, {
    method: 'POST',
    body
  });
}

async function graphFetchWithToken(path, accessToken, options = {}) {
  if (!accessToken) {
    throw new Error('Missing access token.');
  }

  return graphFetch(path, {
    options,
    fallbackMessage: 'Graph API request failed',
    accessTokenRequired: false
  });
}

export async function getPageAccessToken(pageId) {
  const userToken = getCurrentUserToken();

  if (!userToken) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  const page = await graphFetchWithToken(
    `/${pageId}?fields=id,name,access_token&access_token=${encodeURIComponent(userToken)}`,
    userToken
  );

  if (!page) {
    throw new Error(`Không tìm thấy page ${pageId}`);
  }

  if (!page.access_token) {
    throw new Error(`Page ${pageId} không có access_token`);
  }

  return {
    id: page.id,
    name: page.name,
    accessToken: page.access_token
  };
}

export async function listPagePostsWithPageToken(pageId, pageAccessToken, limit = 1) {
  return graphFetchWithToken(
    `/${pageId}/posts?fields=id,message,created_time,permalink_url,status_type&limit=${limit}&access_token=${encodeURIComponent(pageAccessToken)}`,
    pageAccessToken
  );
}

export async function pickFirstValidPostAndCreateAd({
  adAccountId,
  adSetId,
  adName,
  pageId,
  limit = 10
}) {
  const pageInfo = await getPageAccessToken(pageId);

  const postsRes = await listPagePostsWithPageToken(pageId, pageInfo.accessToken, limit);
  const posts = Array.isArray(postsRes?.data) ? postsRes.data : [];

  if (!posts.length) {
    throw new Error(`Không tìm thấy post nào của page ${pageId}`);
  }

  const tried = [];

  for (let i = 0; i < posts.length; i++) {
    const post = posts[i];

    try {
      const ad = await createAdDraftWithObjectStoryId({
        adAccountId,
        adSetId,
        adName,
        objectStoryId: post.id
      });

      return {
        ok: true,
        pickedPost: {
          source: 'auto_first_valid_post',
          index: i + 1,
          id: post.id,
          message: post.message || '',
          created_time: post.created_time || null,
          permalink_url: post.permalink_url || null
        },
        ad
      };
    } catch (err) {
      tried.push({
        index: i + 1,
        postId: post.id,
        error: err.message || 'Unknown error'
      });
    }
  }

  throw new Error(
    `Không có post nào hợp lệ để tạo ad. Tried: ${JSON.stringify(tried)}`
  );
}

export async function updateCampaignStatus({
  campaignId,
  status = 'ACTIVE'
}) {
  const body = new URLSearchParams({
    status,
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${campaignId}`, {
    method: 'POST',
    body
  });
}


export async function deleteCampaign({ campaignId }) {
  const cleanCampaignId = normalizeNumericId(campaignId);
  const token = getCurrentUserToken();

  if (!token) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  if (!cleanCampaignId) {
    throw new Error('Missing campaignId.');
  }

  return metaFetch(
    `/${cleanCampaignId}?access_token=${encodeURIComponent(token)}`,
    {
      method: 'DELETE'
    }
  );
}

export async function updateAdSetStatus({
  adSetId,
  status = 'ACTIVE'
}) {
  const body = new URLSearchParams({
    status,
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${adSetId}`, {
    method: 'POST',
    body
  });
}

export async function updateAdStatus({
  adId,
  status = 'ACTIVE'
}) {
  const body = new URLSearchParams({
    status,
    access_token: getCurrentUserToken()
  });

  return metaFetch(`/${adId}`, {
    method: 'POST',
    body
  });
}
function buildFacebookProfileUrl(idOrUrl) {
  const raw = String(idOrUrl || '').trim();
  if (!raw) return '';

  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    return raw;
  }

  return `https://www.facebook.com/profile.php?id=${encodeURIComponent(raw)}`;
}

function extractPageIdFromInput(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';

  if (/^\d+$/.test(raw)) return raw;

  try {
    const url = new URL(raw);
    const profileId = url.searchParams.get('id');
    if (profileId && /^\d+$/.test(profileId)) return profileId;
  } catch {
    // Not a URL. Fall through to Graph URL resolver.
  }

  return '';
}

async function resolveFacebookPageInput(input) {
  const token = getCurrentUserToken();
  const raw = String(input || '').trim();

  if (!raw) {
    throw new Error('Missing page input.');
  }

  const directId = extractPageIdFromInput(raw);
  const facebookUrl = directId
    ? `https://www.facebook.com/profile.php?id=${directId}`
    : buildFacebookProfileUrl(raw);

  // Ưu tiên resolve URL qua Graph để lấy ID mà API chấp nhận nếu có
  try {
    const data = await metaFetch(
      `/?id=${encodeURIComponent(facebookUrl)}&fields=id,name,link&access_token=${encodeURIComponent(token)}`
    );

    if (data?.id) {
      return {
        input: raw,
        originalPageId: directId || raw,
        resolvedPageId: data.id,
        name: data.name || null,
        link: data.link || facebookUrl,
        source: 'graph_url_resolver'
      };
    }
  } catch (err) {
    // Nếu resolve URL fail thì fallback bên dưới
  }

  if (directId) {
    return {
      input: raw,
      originalPageId: directId,
      resolvedPageId: directId,
      name: null,
      link: facebookUrl,
      source: 'direct_id_fallback'
    };
  }

  throw new Error(`Không resolve được Page ID từ ${raw}`);
}

export async function claimPageToBusiness({ businessId, pageId }) {
  const token = getCurrentUserToken();

  if (!token) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  const cleanBusinessId = String(businessId || '').trim();
  if (!cleanBusinessId) {
    throw new Error('Missing businessId.');
  }

  const resolved = await resolveFacebookPageInput(pageId);

  const body = new URLSearchParams({
    page_id: resolved.resolvedPageId,
    entry_point: 'BUSINESS_SETTINGS',
    access_token: token
  });

  const result = await metaFetch(`/${cleanBusinessId}/owned_pages`, {
    method: 'POST',
    body
  });

  return {
    pageId,
    resolvedPageId: resolved.resolvedPageId,
    name: resolved.name,
    link: resolved.link,
    ok: true,
    status: 'CLAIMED_OR_ADDED_AS_OWNED_PAGE',
    result
  };
}

export async function requestPageAccessToBusiness({
  businessId,
  pageIds = [],
   permittedTasks = ['ADVERTISE', 'ANALYZE', 'CREATE_CONTENT'],
  mode = 'client'
}) {
  const token = getCurrentUserToken();

  if (!token) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  const cleanBusinessId = String(businessId || '').trim();
  const uniquePageIds = [
    ...new Set(
      (pageIds || [])
        .map((x) => String(x || '').trim())
        .filter(Boolean)
    )
  ];

  if (!cleanBusinessId) {
    throw new Error('Missing businessId.');
  }

  if (!uniquePageIds.length) {
    throw new Error('Missing pageIds.');
  }

  const results = [];

  for (const pageId of uniquePageIds) {
    let resolved = null;

    try {
      resolved = await resolveFacebookPageInput(pageId);

      let result;
      let status;

      if (mode === 'client') {
        // Agency/request-access flow: Business requests partner/client access to a Page.
        const body = new URLSearchParams({
          page_id: resolved.resolvedPageId,
          permitted_tasks: JSON.stringify(permittedTasks),
          access_token: token
        });

        result = await metaFetch(`/${cleanBusinessId}/client_pages`, {
          method: 'POST',
          body
        });
        status = 'CLIENT_ACCESS_REQUESTED';
      } else {
        // Owner flow: add/claim an existing Page into this Business.
        // This is the API equivalent closest to UI: “Thêm Trang Facebook có sẵn”.
        const body = new URLSearchParams({
          page_id: resolved.resolvedPageId,
          entry_point: 'BUSINESS_SETTINGS',
          access_token: token
        });
        result = await metaFetch(`/${cleanBusinessId}/owned_pages`, {
          method: 'POST',
          body
        });
        status = 'CLAIMED_OR_ADDED_AS_OWNED_PAGE';
      }

      results.push({
        pageId,
        resolvedPageId: resolved.resolvedPageId,
        name: resolved.name,
        link: resolved.link,
        ok: true,
        status,
        result
      });
    } catch (err) {
      results.push({
        pageId,
        resolvedPageId: resolved?.resolvedPageId || null,
        name: resolved?.name || null,
        link: resolved?.link || buildFacebookProfileUrl(pageId),
        ok: false,
        status: err.errorType || 'FAILED',
        error: err.message || 'Unknown error',
        meta: err.meta || null
      });
    }
  }

  return {
    ok: results.every((x) => x.ok),
    mode,
    businessId: cleanBusinessId,
    total: results.length,
    success: results.filter((x) => x.ok).length,
    failed: results.filter((x) => !x.ok).length,
    results
  };
}

export async function listBusinessOwnedPages({ businessId }) {
  const token = getCurrentUserToken();
  const cleanBusinessId = String(businessId || '').trim();

  if (!token) {
    throw new Error('Missing user token. Please connect Facebook again.');
  }

  if (!cleanBusinessId) {
    throw new Error('Missing businessId.');
  }

  return fetchAllPages(
    `/${cleanBusinessId}/owned_pages?fields=id,name,link&limit=100&access_token=${encodeURIComponent(token)}`
  );
}
