// Uses an offline synthetic report with at least one cached avatar.
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
(async()=>{
 const source=path.resolve(process.argv[2]),moved=path.resolve(process.argv[3]);
 fs.mkdirSync(moved,{recursive:false});
 fs.copyFileSync(source,path.join(moved,path.basename(source)));
 fs.cpSync(path.join(path.dirname(source),'report_assets'),path.join(moved,'report_assets'),{recursive:true});
 const browser=await chromium.launch({executablePath:process.argv[4],headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
 let context=await browser.newContext({offline:true}),page=await context.newPage();
 const errors=[],requests=[];
 const watch=p=>{p.on('pageerror',e=>errors.push(e.message));p.on('request',r=>{if(/^https?:/.test(r.url()))requests.push(r.url())})};
 watch(page);const url='file://'+path.join(moved,path.basename(source));
 await page.goto(url);await page.waitForFunction(()=>document.querySelector('#table-body .avatar img')?.naturalWidth>0);
 const asset=await page.locator('#table-body .avatar img').first().getAttribute('src');
 await page.locator('#table-body .view').first().click();
 await page.waitForFunction(()=>document.querySelector('#modal-avatar img')?.naturalWidth>0);
 await page.keyboard.press('Escape');await page.setViewportSize({width:390,height:844});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 const dimensions=await page.locator('#table-body .avatar').evaluateAll(nodes=>nodes.map(n=>[n.getBoundingClientRect().width,n.getBoundingClientRect().height]));
 assert.ok(dimensions.every(([w,h])=>w===40&&h===40));
 await context.close();fs.unlinkSync(path.join(moved,asset));
 context=await browser.newContext({offline:true});page=await context.newPage();watch(page);
 await page.goto(url);await page.waitForFunction(()=>document.querySelector('#table-body .avatar') && !document.querySelector('#table-body .avatar img'));
 assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
 console.log('Passed: moved HTML/assets only, offline table/modal avatars, mobile dimensions, missing-image fallback, no HTTP traffic.');
 await browser.close();
})().catch(e=>{console.error(e);process.exit(1)});
