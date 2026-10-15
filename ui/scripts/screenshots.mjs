// Captures each dashboard tab in light and dark mode, plus a phone-width view.
// Run inside the Playwright image (browsers preinstalled):
//   node scripts/screenshots.mjs <base-url> <out-dir>
import { mkdirSync } from "node:fs";

import { chromium } from "@playwright/test";

const [baseUrl = "http://localhost:13000", outDir = "screenshots"] = process.argv.slice(2);
const tabs = ["pipeline", "market", "quality", "ml"];

mkdirSync(outDir, { recursive: true });
const browser = await chromium.launch();
try {
  for (const colorScheme of ["light", "dark"]) {
    const page = await browser.newPage({ viewport: { width: 1360, height: 1000 }, colorScheme });
    for (const tab of tabs) {
      await page.goto(`${baseUrl}/#/${tab}`);
      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(800); // let charts paint
      await page.screenshot({ path: `${outDir}/${tab}-${colorScheme}.png`, fullPage: true });
    }
    await page.close();
  }
  const phone = await browser.newPage({ viewport: { width: 390, height: 844 }, colorScheme: "light" });
  for (const tab of ["pipeline", "market"]) {
    await phone.goto(`${baseUrl}/#/${tab}`);
    await phone.waitForLoadState("networkidle");
    await phone.waitForTimeout(800);
    await phone.screenshot({ path: `${outDir}/${tab}-phone.png`, fullPage: true });
  }
} finally {
  await browser.close();
}
console.log(`screenshots written to ${outDir}`);
