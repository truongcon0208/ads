import { chromium } from 'playwright';
import fs from 'fs';

const BUSINESS_ID = process.env.BUSINESS_ID;

if (!BUSINESS_ID) {
  console.error('Missing BUSINESS_ID env');
  process.exit(1);
}

const ids = fs
  .readFileSync('fallback-ui-ids.txt', 'utf8')
  .split(/\r?\n/)
  .map(x => x.trim())
  .filter(Boolean);

const sleep = ms => new Promise(r => setTimeout(r, ms));

const browserContext = await chromium.launchPersistentContext('./fb-session', {
  headless: false,
  viewport: { width: 1400, height: 900 },
  slowMo: 120
});

const page = await browserContext.newPage();

await page.goto(
  `https://business.facebook.com/latest/settings/pages?business_id=${BUSINESS_ID}`,
  { waitUntil: 'domcontentloaded' }
);

console.log('Login Facebook/Business nếu chưa login.');
console.log('Sau khi vào đúng trang Business Settings > Pages, chờ script chạy...');
await sleep(15000);

const results = [];

for (const id of ids) {
  const pageUrl = `https://www.facebook.com/profile.php?id=${id}`;

  console.log(`\n⏳ Add Page UI: ${id}`);

  try {
    await page.getByRole('button', { name: /Thêm|Add/i }).first().click();
    await sleep(1000);

    await page
      .getByText(/Thêm Trang Facebook có sẵn|Add an existing Facebook Page/i)
      .first()
      .click();

    await sleep(1500);

    const input = page.locator('input').first();
    await input.waitFor({ state: 'visible', timeout: 10000 });
    await input.fill(pageUrl);

    await sleep(2500);

    await page.keyboard.press('ArrowDown');
    await page.keyboard.press('Enter');

    await sleep(1200);

    const nextButton = page.getByRole('button', {
      name: /Tiếp|Next|Thêm Trang|Add Page|Xác nhận|Confirm/i
    }).last();

    await nextButton.click();

    await sleep(2500);

    const doneButton = page.getByRole('button', {
      name: /Xong|Done|Hoàn tất|Finish/i
    }).last();

    if (await doneButton.isVisible({ timeout: 4000 }).catch(() => false)) {
      await doneButton.click();
    }

    console.log(`✅ OK: ${id}`);
    results.push({ id, status: 'ok', url: pageUrl });
  } catch (err) {
    console.log(`❌ ${id}: ${err.message}`);
    results.push({
      id,
      status: 'error',
      url: pageUrl,
      error: err.message
    });

    const closeBtn = page.getByRole('button', {
      name: /Đóng|Close|Hủy|Cancel/i
    }).last();

    if (await closeBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      await closeBtn.click().catch(() => {});
    }
  }

  fs.writeFileSync(
    'ui-add-results.json',
    JSON.stringify(results, null, 2)
  );

  await sleep(2500);
}

console.log('\n✅ Done UI fallback');