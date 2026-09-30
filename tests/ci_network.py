"""Matched production-dispatcher TLS matrix; each case is independently preserved."""
import ast
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
from zipfile import ZipFile

repo = Path.cwd()
run_id = os.environ.get("GITHUB_RUN_ID", "local")
evidence = repo / "verification_runs" / run_id / "network"
evidence.mkdir(parents=True, exist_ok=True)
work = Path(tempfile.mkdtemp(prefix="network-verification-", dir=os.environ.get("RUNNER_TEMP")))
final = work / "final"
shutil.copytree(repo, final, ignore=shutil.ignore_patterns(".git", "verification_runs", "__pycache__"))
sources = {"final": final}
for label in ("original", "first_hybrid"):
    dest = work / label
    with ZipFile(repo / "development_sources" / (label + ".zip")) as archive:
        archive.extractall(dest)
    sources[label] = min(dest.rglob("tiktok_worker_scanner.py"), key=lambda p: len(p.parts)).parent

def publish(message):
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    def git(*args, check=True):
        return subprocess.run(["git", *args], cwd=repo, check=check)
    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    git("add", "--", str(evidence.relative_to(repo)))
    if git("diff", "--cached", "--quiet", check=False).returncode == 0:
        return
    git("commit", "-m",  "Network comparison " + run_id + ": " + message)
    for _ in range(5):
        git("pull", "--rebase", "origin", "main")
        if git("push", "origin", "HEAD:main", check=False).returncode == 0:
            return
    raise RuntimeError("Could not publish evidence; retained in artifact")

provenance = {
    "commit": os.environ.get("GITHUB_SHA"), "run_id": run_id,
    "python": platform.python_version(), "platform": platform.platform(),
    "source_sha256": {k: hashlib.sha256((v / "tiktok_worker_scanner.py").read_bytes()).hexdigest()
                      for k, v in sources.items()},
    "scope": "Real verified loopback TLS with synthetic origin, no live TikTok claims",
}
(evidence / "provenance.json").write_text(json.dumps(provenance, indent=2))
publish("source provenance")
results = []
for label, source in sources.items():
    modes = ("worker",) if label == "original" else ("worker", "direct", "hybrid")
    for mode in modes:
        for workers in (100, 500, 1000, 2500, 5000):
            name = label + "_" + mode + "_" + str(workers)
            command = [sys.executable, str(final / "tests/benchmark_network_scaling.py"),
                       "--source", str(source), "--mode", mode, "--workers", str(workers),
                       "--result", str(evidence / (name + ".json"))]
            if label == "original":
                command.append("--original")
            with (evidence / (name + ".log")).open("w") as log:
                completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=600)
            results.append({"variant": label, "mode": mode, "workers": workers, "returncode": completed.returncode})
            (evidence / "summary.json").write_text(json.dumps({"provenance": provenance, "results": results}, indent=2))
            publish(name + " exit=" + str(completed.returncode))
            if completed.returncode:
                raise RuntimeError(name + " failed; log preserved")
(evidence / "completion.json").write_text(json.dumps({"status": "COMPLETED", **provenance}, indent=2))
publish("all 35 cases completed")
