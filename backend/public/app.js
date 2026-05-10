const $ = (id) => document.getElementById(id);

const els = {
  backendUrl: $('backendUrl'),
  adAccountId: $('adAccountId'),
  objective: $('objective'),
  postId: $('postId'),
  defaultPageName: $('defaultPageName'),
  defaultBudget: $('defaultBudget'),
  batchDelayMs: $('batchDelayMs'),
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
  permissionInput: $('permissionInput')
};

const PROGRESS_STORE_KEY = 'botAdsManager.safeRunner.progress.v2';
const PERMISSION_CHUNK_SIZE = 50;
const PERMISSION_CHUNK_DELAY_MS = 1200;

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
    delayMs: getPositiveIntFromInput(els.batchDelayMs, 3000, 0, 300000),
    maxRetries: getPositiveIntFromInput(els.maxRetries, 4, 0, 10),
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

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, options);
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
      lastError = err;

      if (!isRetryableApiError(err) || attempt >= maxRetries) {
        throw err;
      }

      const waitMs = backoffDelayMs(attempt, baseDelayMs);
      appendStatus(`${label} bị limit/lỗi mạng, retry lần ${attempt + 1}/${maxRetries} sau ${Math.round(waitMs / 1000)}s`, 'running');
      await sleep(waitMs);
    }
  }

  throw lastError || new Error(`${label} thất bại`);
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
      await sleep(PERMISSION_CHUNK_DELAY_MS);
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

async function runFullFlow() {
  if (running) return;
  running = true;
  els.runFullFlowBtn.disabled = true;
  const oldButtonText = els.runFullFlowBtn.textContent;
  els.runFullFlowBtn.textContent = 'Đang chạy an toàn...';

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
    appendStatus(`Chế độ an toàn: chạy lần lượt từng ID | delay ${settings.delayMs}ms | retry ${settings.maxRetries} lần | resume ${settings.resumeSuccessful ? 'bật' : 'tắt'}`, 'section');

    const permissionScan = await scanPermissionsForItems(items, { render: true, settings });
    if (!permissionScan.adAccount?.ok) {
      throw new Error(`Ad Account chưa có quyền trong token/BM: ${els.adAccountId.value.trim()}`);
    }

    const blockedPageMap = new Map(
      (permissionScan.pages || [])
        .filter((x) => !x.ok)
        .map((x) => [String(x.pageId), x])
    );

    const summary = {
      success: 0,
      failed: 0,
      skippedNoPermission: 0,
      skippedAlreadyDone: 0
    };

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const payload = buildPayloadFromFirstItem(item);

      appendDivider();
      appendStatus(`Dòng ${i + 1}/${items.length}: ${item.pageName} (${item.pageId})`, 'running');

      const blocked = blockedPageMap.get(String(item.pageId));
      if (blocked) {
        summary.skippedNoPermission += 1;
        appendStatus(`${item.pageName} - SKIP: page chưa cấp quyền vào Business/token`, 'error');
        markProgress(payload, { ok: false, status: 'skipped_no_permission', error: blocked.reason || blocked.error || 'No permission' });
        continue;
      }

      const existingProgress = getProgress(payload);
      if (settings.resumeSuccessful && existingProgress?.ok) {
        summary.skippedAlreadyDone += 1;
        appendStatus(`${item.pageName} - SKIP: đã thành công trước đó (${existingProgress.adId || existingProgress.adSetId || existingProgress.campaignId || 'done'})`, 'success');
        continue;
      }

      try {
        const data = await fetchJsonWithRetry(`${getBackendUrl()}/flow/run-full-draft`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        }, {
          maxRetries: settings.maxRetries,
          baseDelayMs: settings.delayMs,
          label: `Page ${item.pageId}`
        });

        summary.success += 1;
        markProgress(payload, {
          ok: true,
          status: 'success',
          campaignId: data?.campaign?.id || null,
          adSetId: data?.adSet?.id || null,
          adId: data?.ad?.id || null,
          adsManagerUrl: data?.adsManagerUrl || null
        });

        if (data.adsManagerUrl) {
          appendNameLink(item.pageName, data.adsManagerUrl);
        } else {
          appendStatus(`${item.pageName} - Thành công`, 'success');
        }
      } catch (err) {
        summary.failed += 1;
        const errorType = err?.errorType ? ` [${err.errorType}]` : '';
        const backendData = err?.data || null;
        const errorMessage = backendData?.error || err.message || 'Lỗi backend';
        markProgress(payload, { ok: false, status: 'failed', error: errorMessage, errorType: err?.errorType || null });
        appendStatus(`${item.pageName} - ${errorMessage}${errorType}`, 'error');
      }

      if (settings.delayMs > 0 && i < items.length - 1) {
        await sleep(settings.delayMs);
      }
    }

    appendDivider();
    appendStatus(`Tổng kết: SUCCESS ${summary.success} | SKIP_DONE ${summary.skippedAlreadyDone} | SKIP_NO_PERMISSION ${summary.skippedNoPermission} | FAILED ${summary.failed}`, 'section');
    appendStatus('Đã chạy xong tất cả các dòng.', 'section');
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
els.openAdsManagerBtn.addEventListener('click', openAdsManager);
els.backendUrl.addEventListener('change', checkFacebookAuth);

checkFacebookAuth();
