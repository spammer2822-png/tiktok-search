# Rebuild checkpoint — 2026-09-28

WORK IN PROGRESS. Not a final release.

Completed: restored avatar fix and upstream commit; read both specs fully; original scanner 5 worker-count baselines; native signer unchanged; Direct normalizer/session; first hybrid engine, backend metrics, dynamic routing, cursor ownership/restart, configuration. Original 170 regression tests passed after integration edits. New tests and first hybrid benchmark are running; see hybrid_measurements.

Remaining: finish integration gaps (worker-only metrics, per-proxy aggregate admission, connection measurement, retry starvation, raw Direct profile), full first-hybrid benchmark/review/profile/optimization; then implement new 429 controller and stress tests; full final benchmark/review/docs/checklist. Do not claim finished or live verified.

Upstream original archive was truncated in prior ZIP. Rebuilt a valid archive from exact git commit 737bf3dfe9de1dbff57990c0ec4c9e02c75c3d0f. Restored upstream signing/parser test subset: 371 pass. Earlier baseline logs preserved. Both Markdown files in specifications are authoritative.
