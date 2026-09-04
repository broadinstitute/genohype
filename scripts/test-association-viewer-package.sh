#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${1:-$repo_root/target/release/genohype}"
index="$repo_root/cli/static/association-viewer/dist/index.html"

if [[ ! -x "$binary" ]]; then
    echo "error: release binary is not executable: $binary" >&2
    exit 1
fi
if [[ ! -s "$index" ]] || grep -q 'data-genohype-viewer-fallback' "$index"; then
    echo "error: production association viewer index is missing or is the fallback" >&2
    exit 1
fi
if ! grep -Eq '(src|href)="\./assets/[^\"]+-[A-Za-z0-9_-]{8,}\.(js|css)"' "$index"; then
    echo "error: production viewer index lacks checksum-addressed assets" >&2
    exit 1
fi

REPO_ROOT="$repo_root" BINARY="$binary" python3 <<'PY'
import os, re, select, shlex, signal, subprocess, tempfile, time, urllib.request
from pathlib import Path

repo = Path(os.environ["REPO_ROOT"])
binary = os.environ["BINARY"]
runner = shlex.split(os.environ.get("GENOHYPE_BINARY_RUNNER", ""))
with tempfile.TemporaryDirectory(prefix="genohype-viewer-package-") as temporary:
    root = Path(temporary)
    env = os.environ.copy()
    env.update({"HOME": str(root / "home"), "XDG_CONFIG_HOME": str(root / "config"), "XDG_CACHE_HOME": str(root / "cache")})
    command = runner + [binary, "manhattan", "--table", str(repo / "data/test/association/single-variant.ht"), "--y-field", "Pvalue", "--output", str(root / "plot"), "--width", "800", "--height", "400", "--view", "--no-open", "--port", "0"]
    process = subprocess.Popen(command, cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    url = None
    output = []
    deadline = time.time() + 60
    try:
        while time.time() < deadline:
            if process.poll() is not None:
                break
            ready, _, _ = select.select([process.stdout], [], [], 0.25)
            if ready:
                line = process.stdout.readline()
                output.append(line)
                match = re.search(r"Viewer URL: (http://127\.0\.0\.1:\d+/[0-9a-f-]+/)", line)
                if match:
                    url = match.group(1)
                    break
        if not url:
            stderr = process.stderr.read() if process.poll() is not None else ""
            raise RuntimeError(f"viewer URL was not printed; stdout={''.join(output)!r}; stderr={stderr!r}")
        with urllib.request.urlopen(url, timeout=5) as response:
            html = response.read().decode("utf-8")
        if "data-genohype-viewer-fallback" in html:
            raise RuntimeError("release binary served fallback viewer HTML")
        match = re.search(r'(?:src|href)="\./assets/([^\"]+-[A-Za-z0-9_-]{8,}\.(?:js|css))"', html)
        if not match:
            raise RuntimeError("served production HTML has no checksum-addressed asset")
        with urllib.request.urlopen(url + "assets/" + match.group(1), timeout=5) as response:
            asset = response.read()
            cache = response.headers.get("Cache-Control", "")
        if not asset or "immutable" not in cache:
            raise RuntimeError("embedded checksum-addressed asset is empty or not immutable")
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.returncode not in (0, None):
            stderr = process.stderr.read()
            raise RuntimeError(f"viewer process exited with {process.returncode}: {stderr}")
print("Production association viewer embedding verified.")
PY
