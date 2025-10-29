const fs = require('fs');
const { chromium } = require('playwright');

(async () => {
  const url = process.argv[2] || 'http://localhost:3001/';
  const out = process.argv[3] || 'screenshots/home.png';
  await fs.promises.mkdir(require('path').dirname(out), { recursive: true });
  const browser = await chromium.launch({ args: ['--no-sandbox'], headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
    page.on('console', msg => console.log('PAGE LOG:', msg.text()));
    await page.goto(url, { waitUntil: 'networkidle' });
    // wait a little for client JS to hydrate and render charts
    await page.waitForTimeout(1000);
    await page.screenshot({ path: out, fullPage: true });
    console.log('Screenshot saved to', out);
  } catch (err) {
    console.error('screenshot error', err);
    process.exitCode = 2;
  } finally {
    await browser.close();
  }
})();
