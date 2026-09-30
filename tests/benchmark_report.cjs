/* Local file:// UI regression + timings. Requires Playwright, no server/network.
   node tests/benchmark_report.cjs /path/report.html /path/chromium [fallback]
   PLAYWRIGHT_MODULE can point to the installed Playwright package. */
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const assert=require('node:assert/strict');
const path=require('node:path');
const fs=require('node:fs');
const report=path.resolve(process.argv[2]);
const fallback=process.argv[4]==='fallback';
(async()=>{
 const browser=await chromium.launch({executablePath:process.argv[3],headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
 const context=await browser.newContext({offline:true,viewport:{width:1600,height:1000}});
 const page=await context.newPage();const errors=[],network=[];
 page.setDefaultTimeout(120000);
 page.on('pageerror',e=>{errors.push(e.message);console.error('PAGE ERROR:',e.message)});page.on('request',r=>{if(/^https?:/.test(r.url()))network.push(r.url())});
 await page.addInitScript(({fallback})=>{
   window.lags=[];window.longTasks=[];
   let last=performance.now();setInterval(()=>{const now=performance.now();window.lags.push(Math.max(0,now-last-20));last=now},20);
   new PerformanceObserver(list=>window.longTasks.push(...list.getEntries().map(e=>e.duration))).observe({entryTypes:['longtask']});
   if(fallback)window.Worker=class{constructor(){throw Error('synthetic Worker restriction')}};
 },{fallback});
 const ready=()=>page.waitForFunction(()=>document.querySelector('#table-body tr') && document.querySelector('#table-body').getAttribute('aria-busy')!=='true' && !/Updating|Indexing/.test(document.querySelector('#result-count').textContent));
 const measure=async action=>{const start=performance.now();await action();await ready();return Math.round(performance.now()-start)};
 let start=performance.now();await page.goto('file://'+report,{waitUntil:'domcontentloaded'});await ready();
 const result={load_ms:Math.round(performance.now()-start),fallback};
 console.log('Initial table ready in',result.load_ms,'ms');
 result.accounts=Number((await page.locator('#result-count').innerText()).match(/[\d,]+/)[0].replaceAll(',',''));
 assert.equal(await page.locator('#table-body tr').count(),20);
 result.engine=await page.evaluate(()=>document.body.dataset.reportEngine||'original-main-thread');
 if(fallback)assert.equal(result.engine,'cooperative');
 result.initial_max_ui_lag_ms=Math.round(await page.evaluate(()=>Math.max(0,...window.lags)));
 await page.evaluate(()=>{window.lags=[];window.longTasks=[]});
 result.sort_ms=await measure(()=>page.locator('[data-sort=followers]').click());
 console.log('First sort complete in',result.sort_ms,'ms');
 let numbers=await page.locator('#table-body tr td:nth-child(3)').allTextContents();assert.equal(Number(numbers[0].replaceAll(',','')),0);
 result.descending_sort_ms=await measure(()=>page.locator('[data-sort=followers]').click());
 numbers=await page.locator('#table-body tr td:nth-child(3)').allTextContents();assert.ok(Number(numbers[0].replaceAll(',',''))>0);
 // Sorting usernames after a numeric sort avoids an already-sorted best case.
 result.username_sort_ms=await measure(()=>page.locator('[data-sort=username]').click());
 result.page_ms=await measure(()=>page.locator('#next').click());assert.match(await page.locator('#page-label').innerText(),/Page 2 /);
 await measure(()=>page.locator('#page-size').selectOption('500'));assert.equal(await page.locator('#table-body tr').count(),500);
 await measure(()=>page.locator('#page-size').selectOption('20'));
 start=performance.now();await page.locator('#search').fill('synthetic'+String(result.accounts-1).padStart(6,'0'));
 await page.waitForFunction(()=>document.querySelector('#result-count').textContent==='1 result(s)');await ready();
 result.search_ms=Math.round(performance.now()-start);
 await page.locator('#table-body button').first().click();assert.equal(await page.locator('dialog').isVisible(),true);
 assert.match(await page.locator('#modal-fields').innerText(),/Discovered from/);
 assert.equal(await page.locator('#profile-link').getAttribute('rel'),'noopener noreferrer');await page.keyboard.press('Escape');
 await measure(()=>page.locator('#clear').click());
 result.filter_ms=await measure(()=>page.locator('[data-filter=status]').selectOption('partial'));
 assert.match(await page.locator('#table-body').innerText(),/Partial/);
 await measure(()=>page.locator('#clear').click());
 for(const section of ['matches','mutuals','skipped','errors','phases','all'])await measure(()=>page.locator('[data-section='+section+']').click());
 await measure(()=>page.locator('[data-section=mutuals]').click());
 await page.locator('#table-body button').first().click();assert.match(await page.locator('#modal-fields').innerText(),/Confirmed true/);await page.keyboard.press('Escape');
 await measure(()=>page.locator('[data-section=all]').click());
 result.dom_nodes=await page.locator('*').count();assert.ok(result.dom_nodes<2500);
 result.interaction_max_ui_lag_ms=Math.round(await page.evaluate(()=>Math.max(0,...window.lags)));
 result.interaction_long_tasks=await page.evaluate(()=>window.longTasks);
 await page.screenshot({path:report+'.desktop.png'});
 await page.setViewportSize({width:390,height:844});await page.locator('#directory').scrollIntoViewIfNeeded();
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 await page.screenshot({path:report+'.mobile.png',fullPage:true});
 assert.equal(await page.evaluate(()=>window.EXFILTRATED),undefined);assert.deepEqual(errors,[]);assert.deepEqual(network,[]);
 result.network_requests=network.length;result.errors=errors;
 fs.writeFileSync(report+(fallback?'.fallback':'.browser')+'.json',JSON.stringify(result,null,2));
 console.log(JSON.stringify(result,null,2));await browser.close();
})().catch(e=>{console.error(e);process.exit(1)});
