const $ = (id) => document.getElementById(id);

const els = {
  backendUrl: $('backendUrl'),
  adAccountId: $('adAccountId'),
  objective: $('objective'),
  postId: $('postId'),
  defaultPageName: $('defaultPageName'),
  defaultBudget: $('defaultBudget'),
  batchDelayMs: $('batchDelayMs'),
  workerCount: $('workerCount'),
  maxRetries: $('maxRetries'),
  resumeSuccessful: $('resumeSuccessful'),
  batchInput: $('batchInput'),
  permissionCheckBtn: $('permissionCheckBtn'),
  runFullFlowBtn: $('runFullFlowBtn'),
  openAdsManagerBtn: $('openAdsManagerBtn'),
  status: $('status'),
  authStatus: $('authStatus'),
  fbIdentity: $('fbIdentity'),
  loginFacebookBtn: $('loginFacebookBtn'),
  businessId: $('businessId'),
  requestAccessBtn: $('requestAccessBtn'),
  permissionInput: $('permissionInput'),
  deleteCampaignInput: $('deleteCampaignInput'),
  deleteCampaignBtn: $('deleteCampaignBtn')
};

const PROGRESS_STORE_KEY = 'botAdsManager.safeRunner.progress.v2';
const PERMISSION_CHUNK_SIZE = 5;
const PERMISSION_CHUNK_DELAY_MS = 800;
const FETCH_TIMEOUT_MS = 120000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(str) {
  return String(str)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function setStatusHtml(html) {
  els.status.innerHTML = html;
}

function appendStatus(message, type = 'normal') {
  const cls = `log-line log-${type}`;
  const icon =
    type === 'success' ? '✅' :
    type === 'error' ? '❌' :
    type === 'running' ? '⏳' :
    type === 'section' ? '📌' :
    '•';

  els.status.innerHTML += `<div class="${cls}">${icon} ${escapeHtml(message)}</div>`;
  els.status.scrollTop = els.status.scrollHeight;
}

function appendNameLink(name, url) {
  const safeName = escapeHtml(name);
  const safeUrl = escapeHtml(url);

  els.status.innerHTML += `
    <div class="log-line log-success">
      ✅ ${safeName} - <a href="${safeUrl}" target="_blank" rel="noopener noreferrer">Mở link</a>
    </div>
  `;
  els.status.scrollTop = els.status.scrollHeight;
}

function appendDivider() {
  els.status.innerHTML += `<div class="log-divider"></div>`;
}

function getBackendUrl() {
  const url = els.backendUrl.value.trim();
  if (!url) return '';
  return url.replace(/\/$/, '');
}

function getPositiveIntFromInput(input, fallback, min, max) {
  const value = Number(input?.value ?? fallback);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(Math.max(Math.floor(value), min), max);
}

function getRunSettings() {
  return {
    delayMs: getPositiveIntFromInput(els.batchDelayMs, 800, 0, 300000),
    workerCount: getPositiveIntFromInput(els.workerCount, 4, 1, 5),
    maxRetries: getPositiveIntFromInput(els.maxRetries, 3, 0, 10),
    resumeSuccessful: els.resumeSuccessful ? els.resumeSuccessful.checked : true
  };
}

function isRetryableApiError(err) {
  const msg = String(err?.message || '').toLowerCase();
  const type = String(err?.errorType || '').toUpperCase();
  const code = Number(err?.meta?.code);

  return (
    type === 'RATE_LIMIT' ||
    [4, 17, 32, 613, 80004].includes(code) ||
    msg.includes('rate limit') ||
    msg.includes('too many calls') ||
    msg.includes('temporarily blocked') ||
    msg.includes('try again later') ||
    msg.includes('network') ||
    msg.includes('fetch failed') ||
    msg.includes('failed to fetch') ||
    msg.includes('load failed') ||
    msg.includes('networkerror') ||
    msg.includes('connection') ||
    msg.includes('abort') ||
    msg.includes('timeout')
  );
}

function backoffDelayMs(attempt, baseDelayMs) {
  const base = Math.max(1000, Number(baseDelayMs || 3000));
  const delay = Math.min(90000, base * Math.pow(2, attempt));
  const jitter = Math.floor(Math.random() * 1000);
  return delay + jitter;
}

async function fetchJsonWithRetry(url, options = {}, { maxRetries = 4, baseDelayMs = 3000, label = 'request' } = {}) {
  let lastError;
  const { timeoutMs = FETCH_TIMEOUT_MS, ...fetchOptions } = options || {};

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        ...fetchOptions,
        signal: fetchOptions.signal || controller.signal
      });
      clearTimeout(timeoutId);
      let data = null;

      try {
        data = await res.json();
      } catch {
        const err = new Error(`${label} không trả JSON. HTTP ${res.status}`);
        err.status = res.status;
        err.errorType = res.status === 429 || res.status >= 500 ? 'RATE_LIMIT' : 'UNKNOWN';
        throw err;
      }

      if (!res.ok || data?.ok === false) {
        const err = new Error(data?.error || `HTTP ${res.status}`);
        err.status = res.status;
        err.errorType = data?.errorType || (res.status === 429 || res.status >= 500 ? 'RATE_LIMIT' : 'UNKNOWN');
        err.meta = data?.meta || data?.error?.meta || null;
        err.data = data;
        throw err;
      }

      return data;
    } catch (err) {
      clearTimeout(timeoutId);
      lastError = err;

      if (!isRetryableApiError(err) || attempt >= maxRetries) {
        const msg = String(err?.message || '').toLowerCase();
        if (msg.includes('failed to fetch') || msg.includes('fetch failed') || msg.includes('load failed') || msg.includes('networkerror') || msg.includes('abort')) {
          const clearer = new Error(`${label} không kết nối được backend sau ${attempt + 1} lần thử. Kiểm tra backend có đang chạy, URL backend, CORS/HTTPS hoặc rate-limit/server timeout.`);
          clearer.originalError = err;
          clearer.errorType = 'NETWORK';
          throw clearer;
        }
        throw err;
      }

      const waitMs = backoffDelayMs(attempt, baseDelayMs);
      appendStatus(`${label} bị limit/lỗi mạng, retry lần ${attempt + 1}/${maxRetries} sau ${Math.round(waitMs / 1000)}s`, 'running');
      await sleep(waitMs);
    }
  }

  throw lastError || new Error(`${label} thất bại`);
}


function isBackendDisconnectError(err) {
  const msg = String(err?.message || '').toLowerCase();
  const type = String(err?.errorType || '').toUpperCase();

  return (
    type === 'NETWORK' ||
    msg.includes('không kết nối được backend') ||
    msg.includes('failed to fetch') ||
    msg.includes('fetch failed') ||
    msg.includes('load failed') ||
    msg.includes('networkerror') ||
    msg.includes('abort') ||
    msg.includes('timeout') ||
    msg.includes('connection')
  );
}

async function fetchJobStatusForever(url, options = {}, retryOptions = {}) {
  let reconnectCount = 0;
  let reconnectDelayMs = 30000;

  while (true) {
    try {
      const data = await fetchJsonWithRetry(url, options, retryOptions);

      if (reconnectCount > 0) {
        appendStatus('Đã kết nối lại backend, tiếp tục lấy tiến độ job.', 'success');
      }

      return data;
    } catch (err) {
      if (!isBackendDisconnectError(err)) {
        throw err;
      }

      reconnectCount += 1;

      appendStatus(
        `Mất kết nối backend tạm thời. Job có thể vẫn chạy trên Railway. Tự nối lại lần ${reconnectCount} sau ${Math.round(reconnectDelayMs / 1000)}s...`,
        'running'
      );

      await sleep(reconnectDelayMs);

      reconnectDelayMs = Math.min(Math.round(reconnectDelayMs * 1.5), 60000);
    }
  }
}


function renderFacebookIdentity(profile) {
  if (!els.fbIdentity) return;

  if (!profile) {
    els.fbIdentity.style.display = 'none';
    els.fbIdentity.textContent = '';
    return;
  }

  const name = profile.name || 'Không rõ tên';
  const id = profile.id || 'Không rõ ID';

  els.fbIdentity.style.display = 'block';
  els.fbIdentity.textContent = `Đang kết nối: ${name} | Facebook ID: ${id}`;
}

async function checkFacebookAuth() {
  els.authStatus.className = 'auth-status';
  els.authStatus.textContent = 'Đang kiểm tra trạng thái kết nối...';

  try {
    const res = await fetch(`${getBackendUrl()}/auth/status`);
    const data = await res.json();

    if (data?.hasToken) {
      els.authStatus.textContent = 'Đã kết nối Facebook';
      els.authStatus.className = 'auth-status ok';
      els.loginFacebookBtn.textContent = 'Kết nối lại Facebook';
      renderFacebookIdentity(data.profile || null);
    } else {
      els.authStatus.textContent = 'Chưa kết nối Facebook';
      els.authStatus.className = 'auth-status warn';
      els.loginFacebookBtn.textContent = 'Đăng nhập Facebook';
      renderFacebookIdentity(null);
    }
  } catch (_err) {
    els.authStatus.textContent = 'Không kết nối được backend';
    els.authStatus.className = 'auth-status warn';
    renderFacebookIdentity(null);
  }
}

function parseBatchInput(raw) {
  const lines = raw.split('\n').map((s) => s.trim()).filter(Boolean);
  const items = [];
  const errors = [];
  const defaultPageName = (els.defaultPageName?.value || '').trim();
  const budgetRaw = (els.defaultBudget?.value || '').trim();
  const budget = Number(budgetRaw);

  if (!Number.isFinite(budget) || budget <= 0) {
    errors.push(`Budget chung không hợp lệ: ${budgetRaw || '(trống)'}`);
    return { items, errors };
  }

  for (const [index, line] of lines.entries()) {
    const pageId = line.trim();

    if (!pageId) {
      errors.push(`Dòng ${index + 1} thiếu pageId`);
      continue;
    }

    const pageName = defaultPageName || pageId;
    items.push({ pageId, pageName, budget });
  }

  return { items, errors };
}

function parsePageIdsOnly(raw) {
  return [
    ...new Set(
      String(raw || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)
    )
  ];
}

function buildPayloadFromFirstItem(item) {
  return {
    adAccountId: els.adAccountId.value.trim(),
    campaignName: `AUTO ${item.pageName} - ${item.pageId}`,
    adSetName: `Nhóm QC - ${item.pageName} - ${item.pageId}`,
    adName: `Ad - ${item.pageName} - ${item.pageId}`,
    objective: els.objective.value || 'OUTCOME_ENGAGEMENT',
    dailyBudget: item.budget,
    pageId: item.pageId,
    pageName: item.pageName,
    optimizationGoal: 'CONVERSATIONS',
    postId: els.postId.value.trim() || ''
  };
}

function loadProgressStore() {
  try {
    const parsed = JSON.parse(localStorage.getItem(PROGRESS_STORE_KEY) || '{}');
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function saveProgressStore(store) {
  localStorage.setItem(PROGRESS_STORE_KEY, JSON.stringify(store));
}

function makeProgressKey(payload) {
  const raw = JSON.stringify({
    backendUrl: getBackendUrl(),
    adAccountId: payload.adAccountId,
    pageId: payload.pageId,
    dailyBudget: payload.dailyBudget,
    objective: payload.objective,
    postId: payload.postId || ''
  });

  try {
    return btoa(unescape(encodeURIComponent(raw)));
  } catch {
    return raw;
  }
}

function markProgress(payload, record) {
  const store = loadProgressStore();
  store[makeProgressKey(payload)] = {
    ...record,
    updatedAt: new Date().toISOString()
  };
  saveProgressStore(store);
}

function getProgress(payload) {
  const store = loadProgressStore();
  return store[makeProgressKey(payload)] || null;
}

let running = false;

async function scanPermissionsForItems(items, { render = true, settings = getRunSettings() } = {}) {
  const adAccountId = els.adAccountId.value.trim();
  const pageIds = [
    ...new Set(
      items
        .map((item) => String(item.pageId || '').trim())
        .filter(Boolean)
    )
  ];

  if (!adAccountId) {
    throw new Error('Thiếu Ad Account ID.');
  }

  if (!pageIds.length) {
    throw new Error('Không có pageId để check quyền.');
  }

  const merged = {
    ok: true,
    adAccount: null,
    pages: [],
    summary: { totalPages: 0, allowedPages: 0, blockedPages: 0 }
  };

  for (let i = 0; i < pageIds.length; i += PERMISSION_CHUNK_SIZE) {
    const chunk = pageIds.slice(i, i + PERMISSION_CHUNK_SIZE);
    const from = i + 1;
    const to = i + chunk.length;

    if (render) {
      appendStatus(`Check quyền ${from}-${to}/${pageIds.length}`, 'running');
    }

    const data = await fetchJsonWithRetry(`${getBackendUrl()}/permissions/scan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ adAccountId, pageIds: chunk })
    }, {
      maxRetries: settings.maxRetries,
      baseDelayMs: settings.delayMs,
      label: `Check quyền ${from}-${to}`
    });

    if (!merged.adAccount) merged.adAccount = data.adAccount || null;
    merged.pages.push(...(data.pages || []));

    if (data.adAccount && !data.adAccount.ok) {
      merged.ok = false;
    }

    if (i + PERMISSION_CHUNK_SIZE < pageIds.length) {
      await sleep(Math.max(PERMISSION_CHUNK_DELAY_MS, Number(settings.delayMs || 0)));
    }
  }

  const blockedPages = merged.pages.filter((x) => !x.ok);
  const allowedPages = merged.pages.filter((x) => x.ok);
  merged.ok = Boolean(merged.adAccount?.ok) && blockedPages.length === 0;
  merged.summary = {
    totalPages: merged.pages.length,
    allowedPages: allowedPages.length,
    blockedPages: blockedPages.length
  };

  if (render) {
    if (!merged.adAccount?.ok) {
      appendStatus('ACT không có quyền', 'error');
      return merged;
    }

    if (!blockedPages.length) {
      appendStatus(`Tất cả ${allowedPages.length} ID đều có quyền`, 'success');
    } else {
      appendStatus(`${blockedPages.length}/${merged.pages.length} ID không có quyền`, 'error');
      for (const page of blockedPages.slice(0, 80)) {
        appendStatus(`${page.pageId} không có quyền`, 'error');
      }
      if (blockedPages.length > 80) {
        appendStatus(`Còn ${blockedPages.length - 80} ID lỗi quyền, không hiển thị hết để tránh lag log`, 'running');
      }
    }
  }

  return merged;
}

async function checkPermissionsOnly() {
  if (running) return;
  running = true;
  setStatusHtml('');

  try {
    const rawPermissionInput = els.permissionInput?.value || els.batchInput?.value || '';
    const pageIds = parsePageIdsOnly(rawPermissionInput);
    const items = pageIds.map((pageId) => ({
      pageId,
      pageName: pageId,
      budget: Number(els.defaultBudget?.value || 100)
    }));

    if (!items.length) {
      throw new Error('Không có record hợp lệ.');
    }

    appendStatus('Bắt đầu check quyền theo cụm nhỏ', 'section');
    await scanPermissionsForItems(items, { render: true });
    appendDivider();
    appendStatus('Check quyền xong.', 'section');
  } catch (err) {
    appendStatus(err.message, 'error');
  } finally {
    running = false;
  }
}

async function requestPageAccessForItems(pageIds = []) {
  const businessId = els.businessId?.value?.trim();
  const settings = getRunSettings();

  pageIds = [
    ...new Set(
      (pageIds || [])
        .map((id) => String(id || '').trim())
        .filter(Boolean)
    )
  ];

  if (!businessId) {
    throw new Error('Thiếu Business ID.');
  }

  if (!pageIds.length) {
    throw new Error('Không có ID để thêm quyền.');
  }

  const allResults = [];
  appendStatus(`Đang add/claim ${pageIds.length} Page vào Business theo cụm nhỏ...`, 'running');

  for (let i = 0; i < pageIds.length; i += PERMISSION_CHUNK_SIZE) {
    const chunk = pageIds.slice(i, i + PERMISSION_CHUNK_SIZE);
    const from = i + 1;
    const to = i + chunk.length;

    appendStatus(`Add/claim ${from}-${to}/${pageIds.length}`, 'running');

    const data = await fetchJsonWithRetry(`${getBackendUrl()}/permissions/request-page-access`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        businessId,
        pageIds: chunk,
        permittedTasks: ['ADVERTISE', 'ANALYZE', 'CREATE_CONTENT', 'MANAGE'],
        mode: 'claim'
      })
    }, {
      maxRetries: settings.maxRetries,
      baseDelayMs: settings.delayMs,
      label: `Add/claim ${from}-${to}`
    });

    allResults.push(...(data.results || []));
    if (i + PERMISSION_CHUNK_SIZE < pageIds.length) await sleep(PERMISSION_CHUNK_DELAY_MS);
  }

  const failed = allResults.filter((x) => !x.ok);
  const success = allResults.filter((x) => x.ok);

  appendStatus(`Đã add/claim vào Business: ${success.length}/${pageIds.length} ID`, failed.length ? 'running' : 'success');

  for (const item of failed.slice(0, 80)) {
    appendStatus(`${item.pageId} lỗi: ${item.error || item.status}`, 'error');
  }
  if (failed.length > 80) {
    appendStatus(`Còn ${failed.length - 80} lỗi add/claim, không hiển thị hết để tránh lag log`, 'running');
  }

  return { ok: failed.length === 0, total: pageIds.length, success: success.length, failed: failed.length, results: allResults };
}


function parseCampaignIdsInput(raw) {
  const lines = String(raw || '').split('\n').map((s) => s.trim()).filter(Boolean);
  const campaignIds = [];
  const errors = [];

  for (const [index, line] of lines.entries()) {
    let picked = '';

    const campaignLike = line.match(/(?:campaign[_\s-]*id|campaign|camp)[^0-9]*(\d{8,})/i);
    if (campaignLike) picked = campaignLike[1];

    if (!picked) {
      const parentheses = [...line.matchAll(/\((\d{8,})\)/g)].map((m) => m[1]);
      if (parentheses.length) picked = parentheses[parentheses.length - 1];
    }

    if (!picked) {
      const nums = [...line.matchAll(/\d{8,}/g)].map((m) => m[0]);
      if (nums.length === 1) {
        picked = nums[0];
      } else if (nums.length > 1) {
        const longest = nums.slice().sort((a, b) => b.length - a.length)[0];
        const sameLongest = nums.filter((n) => n.length === longest.length);
        if (sameLongest.length === 1 && longest.length >= 15) {
          picked = longest;
        } else {
          errors.push(`Dòng ${index + 1} có nhiều ID, hãy chỉ để campaign_id: ${line}`);
          continue;
        }
      }
    }

    if (!picked || !/^\d{8,}$/.test(picked)) {
      errors.push(`Dòng ${index + 1} không có campaign_id hợp lệ: ${line}`);
      continue;
    }

    campaignIds.push(picked);
  }

  return {
    campaignIds: [...new Set(campaignIds)],
    errors
  };
}

async function deleteCampaignsByApi() {
  if (running) return;
  running = true;
  els.deleteCampaignBtn.disabled = true;
  const oldButtonText = els.deleteCampaignBtn.textContent;
  els.deleteCampaignBtn.textContent = 'Đang tạo job xóa...';

  setStatusHtml('');

  try {
    const settings = getRunSettings();
    const { campaignIds, errors } = parseCampaignIdsInput(els.deleteCampaignInput?.value || '');

    if (errors.length) {
      throw new Error(`Lỗi input campaign_id:\n${errors.slice(0, 30).join('\n')}${errors.length > 30 ? `\n... còn ${errors.length - 30} dòng lỗi` : ''}`);
    }

    if (!campaignIds.length) {
      throw new Error('Chưa có campaign_id hợp lệ để xóa. Dán campaign_id dạng 120244... mỗi dòng 1 ID.');
    }

    appendStatus(`Bắt đầu xóa ${campaignIds.length} campaign bằng API`, 'section');
    appendStatus(`Chế độ job nền: ${settings.workerCount} bàn làm việc | mỗi bàn 1 campaign/lần | delay ${settings.delayMs}ms | retry ${settings.maxRetries}`, 'section');
    appendStatus('Lưu ý: đây là xóa campaign theo campaign_id thật, không phải pageId trong tên campaign.', 'running');

    const data = await fetchJsonWithRetry(`${getBackendUrl()}/campaigns/start-delete-job`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        campaignIds,
        settings: {
          delayMs: settings.delayMs,
          concurrency: settings.workerCount,
          maxRetries: settings.maxRetries
        }
      })
    }, {
      maxRetries: 1,
      baseDelayMs: 1500,
      label: 'Tạo job xóa campaign'
    });

    const jobId = data.job?.id;
    if (!jobId) throw new Error('Backend không trả jobId xóa campaign.');

    appendStatus(`Đã tạo job xóa ${jobId}. Bắt đầu lấy tiến độ...`, 'success');
    els.deleteCampaignBtn.textContent = 'Đang xóa...';

    let fromEventIndex = 0;
    let lastProgressText = '';

    while (true) {
      const statusData = await fetchJobStatusForever(`${getBackendUrl()}/campaigns/delete-job-status/${encodeURIComponent(jobId)}?fromEventIndex=${fromEventIndex}`, {
        method: 'GET',
        timeoutMs: 20000
      }, {
        maxRetries: settings.maxRetries,
        baseDelayMs: Math.max(1500, settings.delayMs || 1500),
        label: 'Lấy tiến độ job xóa'
      });

      const job = statusData.job || {};
      const events = statusData.events || [];
      fromEventIndex = statusData.nextEventIndex ?? fromEventIndex;

      for (const event of events) {
        if (event.type === 'success') {
          appendStatus(event.message || `${event.campaignId} - Đã xóa`, 'success');
        } else if (event.type === 'error') {
          appendStatus(event.message || `${event.campaignId} - Xóa lỗi`, 'error');
        } else if (event.type === 'running') {
          appendStatus(event.message || `Đang xóa campaign ${(event.index || 0) + 1}`, 'running');
        } else {
          appendStatus(event.message || 'Cập nhật job xóa', event.type === 'error' ? 'error' : 'section');
        }
      }

      const progressText = `Tiến độ xóa: ${job.completed || 0}/${job.total || campaignIds.length} | SUCCESS ${job.success || 0} | FAILED ${job.failed || 0}`;
      if (progressText !== lastProgressText) {
        els.deleteCampaignBtn.textContent = `Đang xóa ${job.completed || 0}/${job.total || campaignIds.length}`;
        lastProgressText = progressText;
      }

      if (['done', 'failed', 'cancelled'].includes(job.status)) {
        appendDivider();
        appendStatus(`Tổng kết xóa: SUCCESS ${job.success || 0} | FAILED ${job.failed || 0}`, 'section');
        appendStatus(job.status === 'done' ? 'Đã chạy xong job xóa campaign.' : `Job xóa dừng với trạng thái: ${job.status}`, job.status === 'done' ? 'section' : 'error');
        break;
      }

      await sleep(1500);
    }
  } catch (err) {
    appendStatus(err.message, 'error');
    console.error(err);
  } finally {
    running = false;
    els.deleteCampaignBtn.disabled = false;
    els.deleteCampaignBtn.textContent = oldButtonText;
  }
}

async function runFullFlow() {
  if (running) return;
  running = true;
  els.runFullFlowBtn.disabled = true;
  const oldButtonText = els.runFullFlowBtn.textContent;
  els.runFullFlowBtn.textContent = 'Đang tạo job...';

  setStatusHtml('');

  try {
    const settings = getRunSettings();
    const { items, errors } = parseBatchInput(els.batchInput.value || '');

    if (errors.length) {
      throw new Error(`Lỗi input:\n${errors.join('\n')}`);
    }

    if (!items.length) {
      throw new Error('Không có record hợp lệ.');
    }

    appendStatus(`Bắt đầu chạy ${items.length} dòng`, 'section');
    appendStatus('Full luồng KHÔNG check quyền trước. Dòng nào lỗi quyền sẽ báo lỗi đúng dòng đó.', 'section');
    appendStatus(`Chế độ job nền: ${settings.workerCount} bàn làm việc | mỗi bàn 1 ID/lần | delay ${settings.delayMs}ms | resume ${settings.resumeSuccessful ? 'bật' : 'tắt'}`, 'section');

    const payloads = [];
    let skippedAlreadyDone = 0;

    for (const item of items) {
      const payload = buildPayloadFromFirstItem(item);
      const existingProgress = getProgress(payload);

      if (settings.resumeSuccessful && existingProgress?.ok) {
        skippedAlreadyDone += 1;
        appendStatus(`${item.pageName} - SKIP: đã thành công trước đó (${existingProgress.adId || existingProgress.adSetId || existingProgress.campaignId || 'done'})`, 'success');
        continue;
      }

      payloads.push(payload);
    }

    if (!payloads.length) {
      appendDivider();
      appendStatus(`Không còn dòng nào cần chạy. SKIP_DONE ${skippedAlreadyDone}`, 'section');
      return;
    }

    const data = await fetchJsonWithRetry(`${getBackendUrl()}/flow/start-full-job`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        payloads,
        settings: {
          delayMs: settings.delayMs,
          concurrency: settings.workerCount,
          maxRetries: settings.maxRetries,
          skipped: skippedAlreadyDone
        }
      })
    }, {
      maxRetries: 1,
      baseDelayMs: 1500,
      label: 'Tạo job full luồng'
    });

    const jobId = data.job?.id;
    if (!jobId) throw new Error('Backend không trả jobId.');

    appendStatus(`Đã tạo job ${jobId}. Bắt đầu lấy tiến độ...`, 'success');
    els.runFullFlowBtn.textContent = 'Đang chạy job...';

    let fromEventIndex = 0;
    let lastProgressText = '';

    while (true) {
      const statusData = await fetchJobStatusForever(`${getBackendUrl()}/flow/job-status/${encodeURIComponent(jobId)}?fromEventIndex=${fromEventIndex}`, {
        method: 'GET',
        timeoutMs: 20000
      }, {
        maxRetries: settings.maxRetries,
        baseDelayMs: Math.max(1500, settings.delayMs || 1500),
        label: 'Lấy tiến độ job'
      });

      const job = statusData.job || {};
      const events = statusData.events || [];
      fromEventIndex = statusData.nextEventIndex ?? fromEventIndex;

      for (const event of events) {
        if (event.type === 'success') {
          const payload = event.payload || {};
          const result = event.result || {};
          markProgress(payload, {
            ok: true,
            status: 'success',
            campaignId: result?.campaign?.id || null,
            adSetId: result?.adSet?.id || null,
            adId: result?.ad?.id || null,
            adsManagerUrl: result?.adsManagerUrl || null
          });

          if (event.adsManagerUrl) {
            appendNameLink(event.pageName || event.pageId || 'Page', event.adsManagerUrl);
          } else {
            appendStatus(event.message || `${event.pageName || event.pageId} - Thành công`, 'success');
          }
        } else if (event.type === 'error') {
          const payload = event.payload || {};
          markProgress(payload, {
            ok: false,
            status: 'failed',
            error: event.error || event.message || 'Lỗi backend',
            errorType: event.errorType || null
          });
          appendStatus(event.message || `${event.pageName || event.pageId} - Lỗi`, 'error');
        } else if (event.type === 'running') {
          appendStatus(event.message || `Đang chạy dòng ${(event.index || 0) + 1}`, 'running');
        } else {
          appendStatus(event.message || 'Cập nhật job', event.type === 'error' ? 'error' : 'section');
        }
      }

      const progressText = `Tiến độ job: ${job.completed || 0}/${job.total || payloads.length} | SUCCESS ${job.success || 0} | FAILED ${job.failed || 0} | SKIP_DONE ${skippedAlreadyDone}`;
      if (progressText !== lastProgressText) {
        els.runFullFlowBtn.textContent = `Đang chạy ${job.completed || 0}/${job.total || payloads.length}`;
        lastProgressText = progressText;
      }

      if (['done', 'failed', 'cancelled'].includes(job.status)) {
        appendDivider();
        appendStatus(`Tổng kết: SUCCESS ${job.success || 0} | SKIP_DONE ${skippedAlreadyDone} | FAILED ${job.failed || 0}`, 'section');
        appendStatus(job.status === 'done' ? 'Đã chạy xong job.' : `Job dừng với trạng thái: ${job.status}`, job.status === 'done' ? 'section' : 'error');
        break;
      }

      await sleep(1500);
    }
  } catch (err) {
    appendStatus(err.message, 'error');
    console.error(err);
  } finally {
    running = false;
    els.runFullFlowBtn.disabled = false;
    els.runFullFlowBtn.textContent = oldButtonText;
  }
}

function openAdsManager() {
  window.open(`${getBackendUrl()}/auth/status`, '_blank');
}

els.loginFacebookBtn.addEventListener('click', () => {
  window.open(`${getBackendUrl()}/auth/facebook/start`, '_blank');
});

els.requestAccessBtn?.addEventListener('click', async () => {
  if (running) return;
  running = true;
  try {
    appendDivider();

    const rawPermissionInput = els.permissionInput?.value || els.batchInput?.value || '';
    const pageIds = parsePageIdsOnly(rawPermissionInput);

    if (!pageIds.length) {
      appendStatus('Chưa nhập ID nào trong ô Danh sách pageId.', 'error');
      return;
    }

    await requestPageAccessForItems(pageIds);
    appendStatus('Xong. Bấm Check quyền hoặc kiểm tra Business Owned Pages lại.', 'success');
  } catch (err) {
    appendStatus(`Lỗi add/claim Page: ${err.message || 'Unknown error'}`, 'error');
  } finally {
    running = false;
  }
});

els.permissionCheckBtn.addEventListener('click', checkPermissionsOnly);
els.runFullFlowBtn.addEventListener('click', runFullFlow);
els.deleteCampaignBtn?.addEventListener('click', deleteCampaignsByApi);
els.openAdsManagerBtn.addEventListener('click', openAdsManager);
els.backendUrl.addEventListener('change', checkFacebookAuth);

checkFacebookAuth();
