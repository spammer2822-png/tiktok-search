"""Preserve matched SIGINT/resume results without restarting the main matrix."""
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
import time
from zipfile import ZipFile

repo = Path.cwd()
run_id = os.environ.get("GITHUB_RUN_ID", "local")
evidence = repo / "verification_runs" / run_id / "shutdown"
evidence.mkdir(parents=True, exist_ok=True)
work = Path(tempfile.mkdtemp(prefix="shutdown-verification-", dir=os.environ.get("RUNNER_TEMP")))
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
    git("commit", "-m", "Shutdown verification " + run_id + ": " + message)
    target_branch = os.environ.get("GITHUB_REF_NAME") or "main"
    for attempt in range(8):
        git("pull", "--rebase", "origin", target_branch)
        if git("push", "origin", "HEAD:" + target_branch, check=False).returncode == 0:
            return
        time.sleep(min(0.5 * (2 ** attempt), 5.0))
    raise RuntimeError("Could not publish evidence to " + target_branch + "; retained in artifact")

provenance = {
    "commit": os.environ.get("GITHUB_SHA"), "run_id": run_id,
    "python": platform.python_version(), "platform": platform.platform(),
    "source_sha256": {k: hashlib.sha256((v / "tiktok_worker_scanner.py").read_bytes()).hexdigest()
                      for k, v in sources.items()},
    "scope": "Real subprocess signals and SQLite recovery; mocked pending HTTP request",
}
(evidence / "provenance.json").write_text(json.dumps(provenance, indent=2))
inventory = []
for path in sorted(final.glob("*.py")):
    source = path.read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    inventory.append({
        "path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "lines": len(source.splitlines()),
        "definitions": [{"name": node.name, "line": node.lineno,
                         "end_line": node.end_lineno, "kind": type(node).__name__}
                        for node in ast.walk(tree)
                        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
    })
(evidence / "source_inventory.json").write_text(json.dumps(inventory, indent=2))
publish("source provenance and definition inventory")
results = []
for label, source in sources.items():
    for repeated in (False, True):
        name = label + ("_repeated" if repeated else "_single")
        command = [sys.executable, str(final / "tests/benchmark_shutdown.py"),
                   "--source", str(source), "--result", str(evidence / (name + ".json"))]
        if repeated:
            command.append("--repeat")
        with (evidence / (name + ".log")).open("w") as log:
            completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=150)
        result = {"variant": label, "repeated": repeated, "returncode": completed.returncode}
        if (evidence / (name + ".json")).exists():
            result["measurement"] = json.loads((evidence / (name + ".json")).read_text())
        results.append(result)
        (evidence / "summary.json").write_text(json.dumps({"provenance": provenance, "results": results}, indent=2))
        publish(name + " exit=" + str(completed.returncode))
failures = [r for r in results if r["variant"] == "final" and r["returncode"]]
assert not failures, failures
