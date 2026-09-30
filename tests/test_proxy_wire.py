import asyncio, base64, contextlib, ipaddress, json, ssl, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import tiktok_worker_scanner as s
from test_async_scanner import PROFILE

class ProxyWireTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.headers=[];self.connect_headers=[];self.proxies=[];self.handler_tasks=set()
  key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
  name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')])
  cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)-timedelta(days=1)).not_valid_after(datetime.now(timezone.utc)+timedelta(days=1)).add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False).sign(key,hashes.SHA256()))
  (self.root/'key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()));(self.root/'cert.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
  server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_context.load_cert_chain(self.root/'cert.pem',self.root/'key.pem')
  self.verify=ssl.create_default_context(cafile=str(self.root/'cert.pem'))
  self.origin=await asyncio.start_server(self.origin_handler,'127.0.0.1',0,ssl=server_context);port=self.origin.sockets[0].getsockname()[1]
  self.origin_url=f'https://127.0.0.1:{port}';self.patch=patch.object(s,'WORKER_ORIGIN',self.origin_url);self.patch.start();self.logs=patch.object(s,'console');self.logs.start()
 async def asyncTearDown(self):
  for server in [self.origin]+self.proxies:server.close();await server.wait_closed()
  for task in list(self.handler_tasks):task.cancel()
  await asyncio.gather(*list(self.handler_tasks),return_exceptions=True)
  self.patch.stop();self.logs.stop();self.temp.cleanup()
 async def origin_handler(self,reader,writer):
  task=asyncio.current_task();self.handler_tasks.add(task)
  try:
   data=await reader.readuntil(b'\r\n\r\n');self.headers.append(data.decode());await asyncio.sleep(.02)
   body=json.dumps(PROFILE).encode();writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body)).encode()+b'\r\nConnection: close\r\n\r\n'+body);await writer.drain()
  except (OSError,asyncio.IncompleteReadError):pass
  finally:
   writer.close()
   with contextlib.suppress(Exception):await writer.wait_closed()
   self.handler_tasks.discard(task)
 async def proxy(self,username='wire-private-user',password='wire-private-pass'):
  expected='Basic '+base64.b64encode(f'{username}:{password}'.encode()).decode()
  async def handler(reader,writer):
   task=asyncio.current_task();self.handler_tasks.add(task);remote=None
   try:
    raw=await reader.readuntil(b'\r\n\r\n');text=raw.decode();self.connect_headers.append(text)
    lines=text.split('\r\n');headers=dict(line.split(':',1) for line in lines[1:] if ':' in line)
    auth=next((v.strip() for k,v in headers.items() if k.lower()=='proxy-authorization'),None)
    if auth!=expected:
     writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="test"\r\nContent-Length: 0\r\n\r\n');await writer.drain();return
    authority=lines[0].split()[1];host,port=authority.rsplit(':',1);upstream,remote=await asyncio.open_connection(host,int(port))
    writer.write(b'HTTP/1.1 200 Connection established\r\n\r\n');await writer.drain()
    async def copy(source,dest):
     while data:=await source.read(65536):dest.write(data);await dest.drain()
    tasks=[asyncio.create_task(copy(reader,remote)),asyncio.create_task(copy(upstream,writer))]
    try:await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
    finally:
     for t in tasks:t.cancel()
     await asyncio.gather(*tasks,return_exceptions=True)
   except (OSError,asyncio.IncompleteReadError):pass
   finally:
    if remote:remote.close()
    writer.close()
    with contextlib.suppress(Exception):await writer.wait_closed()
    self.handler_tasks.discard(task)
  server=await asyncio.start_server(handler,'127.0.0.1',0);self.proxies.append(server)
  return s.ProxyConfig('127.0.0.1',server.sockets[0].getsockname()[1],username,password)
 def client(self,configs,proxy_only=False):
  pool=s.ProxyPool(configs,proxy_only=proxy_only);gate=s.AsyncRequestGate(.01,.01,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
  def factory(route,options):return httpx.AsyncClient(**{**options,'verify':self.verify})
  return s.WorkerApiClient(gate,pool,client_factory=factory)
 async def test_real_connect_auth_isolated_from_https_origin(self):
  config=await self.proxy();c=self.client([config])
  async with c:result=await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(result['status'],'ok');self.assertIn('Proxy-Authorization: Basic ',self.connect_headers[0]);self.assertNotIn('proxy-authorization',self.headers[0].lower());self.assertNotIn('authorization:',self.headers[0].lower());self.assertNotIn(config.password,self.headers[0])
 async def test_real_two_proxies_concurrently(self):
  c=self.client([await self.proxy(),await self.proxy()])
  async with c:await asyncio.gather(c.request_json('profile',{'username':'roblox'}),c.request_json('profile',{'username':'roblox'}))
  self.assertEqual(len(self.connect_headers),2);self.assertTrue(all(p.success_count==1 for p in c.pool.states));self.assertGreaterEqual(c.gate.peak_active,2)
 async def test_real_407_then_direct_fallback(self):
  valid=await self.proxy();bad=s.ProxyConfig(valid.host,valid.port,valid.username,'bad-wire-password');c=self.client([bad])
  # Remove only retry sleeping; actual network, proxy auth, and TLS remain real.
  with patch.object(c.gate,'wait',return_value=None):
   async with c:result=await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(result['status'],'ok');self.assertTrue(c.pool.states[0].disabled);self.assertEqual(c.pool.states[0].last_failure,'proxy_authentication_failure');self.assertEqual(len(self.headers),1)

if __name__=='__main__':unittest.main(verbosity=2)
