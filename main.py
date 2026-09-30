"""Start the TikTok scanner. Keep all supplied project files together."""
import sys
sys.dont_write_bytecode = True
if sys.version_info < (3, 11):
    raise SystemExit('Python 3.11 or newer is required. Run: py -3.11 main.py')
from tiktok_worker_scanner import main
if __name__ == '__main__':
    raise SystemExit(main())
