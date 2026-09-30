Source: https://github.com/Evil0ctal/Douyin_TikTok_Download_API
Commit: 737bf3dfe9de1dbff57990c0ec4c9e02c75c3d0f (v5.1.1)
License: Apache-2.0; see LICENSE.Evil0ctal.

tiktok_sign.py is an unmodified copy of src/dtk/signing/native/tiktok_sign.py.
direct_protocol.py adapts the endpoint parameters and TikTok field extraction
from src/dtk/platforms/tiktok/{params,parser}.py. It adds private-account fields,
strict missing-page detection, raw-member preservation and stable device IDs.
The full DTK service and its Python >=3.12 infrastructure are not runtime
dependencies. The scanner remains compatible with Python 3.11.

Upstream validation logs, configuration and component benchmarks are preserved
in hybrid_baseline. Network-dependent baseline checks are not claimed to pass.
