from pathlib import Path
import zipfile, shutil
root=Path(__file__).parent;project=root/'TikTokScanner';archives=project/'development_sources';archives.mkdir(exist_ok=True)
for label in ('original','first_hybrid'):
 target=archives/(label+'.zip')
 if target.exists():continue
 with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as z:
  for p in (root/label).rglob('*'):
   if p.is_file() and not any(part in ('__pycache__','hybrid_baseline','hybrid_measurements','verification') for part in p.relative_to(root/label).parts):z.write(p,p.relative_to(root/label))
for p in (root/'measured').glob('*'):
 if p.is_file():shutil.copy2(p,project/'hybrid_measurements'/p.name)
shutil.copy2(__file__,project/'development_sources'/'save_checkpoint.py')
destination=root/'TikTokScanner_hybrid_checkpoint.zip'
with zipfile.ZipFile(destination,'w',zipfile.ZIP_DEFLATED) as z:
 for p in project.rglob('*'):
  if p.is_file() and '__pycache__' not in p.parts:z.write(p,Path('TikTokScanner')/p.relative_to(project))
print(destination.resolve(),destination.stat().st_size)
