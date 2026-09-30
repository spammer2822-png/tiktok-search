// All report sections share the same saved avatar, offline, in both engines.
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const assert=require('node:assert/strict'), path=require('node:path');
(async()=>{
 const browser=await chromium.launch({executablePath:process.argv[3],headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
 const context=await browser.newContext({offline:true,viewport:{width:1500,height:1100}});
 const page=await context.newPage(),errors=[],remote=[],sources=new Map();
 page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(/^https?:/.test(r.url()))remote.push(r.url())});
 if(process.argv[4]==='fallback')await page.addInitScript(()=>{window.Worker=class{constructor(){throw Error('synthetic Worker restriction')}}});
 await page.goto('file://'+path.resolve(process.argv[2]));
 const ready=()=>page.waitForFunction(()=>document.body.dataset.reportReady==='true'&&document.querySelector('#table-body').getAttribute('aria-busy')!=='true');
 await ready();
 const expected={overview:['failed','matched','noimage','partial','pending','skipped'],all:['failed','matched','noimage','partial','pending','skipped'],errors:['failed','partial'],skipped:['skipped'],matches:['matched'],mutuals:['matched']};
 for(const [section,names] of Object.entries(expected)){
  await page.locator('[data-section='+section+']').click();await ready();
  assert.deepEqual(await page.locator('#table-body .username').allTextContents(),names.map(n=>'@'+n));
  for(const name of names){
   const row=page.locator('#table-body tr').filter({has:page.locator('.username',{hasText:'@'+name})});
   await row.scrollIntoViewIfNeeded();
   if(name==='noimage'){assert.equal(await row.locator('.avatar img').count(),0);continue;}
   await row.locator('.avatar img').evaluate(img=>img.decode());
   const src=await row.locator('.avatar img').getAttribute('src');
   assert.match(src,/^report_assets\/avatars\/[a-f0-9]{64}\.jpg$/);
   if(sources.has(name))assert.equal(src,sources.get(name));else sources.set(name,src);
   await row.locator('.view').click();await page.locator('#modal-avatar img').evaluate(img=>img.decode());
   assert.equal(await page.locator('#modal-avatar img').getAttribute('src'),src);await page.keyboard.press('Escape');
  }
 }
 await page.locator('[data-section=phases]').click();assert.equal(await page.locator('#phase-panels').isVisible(),true);
 await page.locator('[data-section=all]').click();await ready();
 await page.locator('#search').fill('matched');await page.waitForFunction(()=>document.querySelector('#result-count').textContent==='1 result(s)');
 await page.locator('#table-body img').evaluate(img=>img.decode());
 await page.locator('#clear').click();await ready();
 await page.locator('#page-size').selectOption('500');await ready();assert.equal(await page.locator('#table-body tr').count(),6);
 await page.locator('[data-sort=username]').click();await ready();assert.equal(await page.locator('#table-body .username').first().innerText(),'@skipped');
 await page.locator('[data-filter=status]').selectOption('network_error');await ready();assert.equal(await page.locator('#table-body .username').innerText(),'@failed');
 await page.locator('#clear').click();await ready();
 await page.screenshot({path:process.argv[2]+'.avatars.png',fullPage:true});
 await page.setViewportSize({width:390,height:844});await page.locator('#directory').scrollIntoViewIfNeeded();
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 const sizes=await page.locator('#table-body .avatar').evaluateAll(xs=>xs.map(x=>[x.getBoundingClientRect().width,x.getBoundingClientRect().height]));
 assert.ok(sizes.every(([w,h])=>w===40&&h===40));assert.deepEqual(errors,[]);assert.deepEqual(remote,[]);
 console.log(JSON.stringify({sections:Object.keys(expected),cached_accounts:sources.size,missing_url_placeholder:true,engine:await page.evaluate(()=>document.body.dataset.reportEngine),external_requests:remote.length,errors}));
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1)});
