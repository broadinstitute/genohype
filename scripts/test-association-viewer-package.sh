#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${1:-$repo_root/target/release/genohype}"
embedded="$repo_root/cli/static/association-viewer/dist"
index="$embedded/index.html"

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
import json, os, queue, re, shlex, signal, subprocess, sys, tempfile, threading, time
import urllib.error, urllib.parse, urllib.request
from pathlib import Path

repo = Path(os.environ["REPO_ROOT"])
binary = os.environ["BINARY"]
embedded = repo / "cli/static/association-viewer/dist"
runner = shlex.split(os.environ.get("GENOHYPE_BINARY_RUNNER", ""))
fixture = repo / "data/test/association/single-variant.tsv"
mapping = "contig=CHR,position=POS,ref=REF,alt=ALT,pvalue=P,neg_log10_p=NEG_LOG10_P,beta=BETA,se=SE,af=AF,ac=AC,series=SERIES"

def fetch(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers

def checked_json(url, maximum):
    status, body, headers = fetch(url)
    if status != 200:
        raise RuntimeError(f"GET {url} returned {status}: {body[:200]!r}")
    if len(body) > maximum:
        raise RuntimeError(f"GET {url} exceeded {maximum} bytes")
    if "application/json" not in headers.get("Content-Type", ""):
        raise RuntimeError(f"GET {url} did not return JSON")
    return json.loads(body), body

with tempfile.TemporaryDirectory(prefix="genohype-viewer-package-") as temporary:
    root = Path(temporary)
    home, config, cache = root / "home", root / "config", root / "cache"
    for directory in (home, config, cache, home / "Library", home / "Library" / "Caches"):
        directory.mkdir(mode=0o700)
    cache_base = home / "Library" / "Caches" if sys.platform == "darwin" else cache
    association_cache = cache_base / "genohype" / "association" / "v1"
    env = os.environ.copy()
    env.update({"HOME": str(home), "XDG_CONFIG_HOME": str(config), "XDG_CACHE_HOME": str(cache)})
    command = runner + [
        binary, "association", "view", str(fixture), "--build", "GRCh38",
        "--map", mapping, "--ephemeral", "--no-open", "--port", "0",
    ]
    process = subprocess.Popen(command, cwd=repo, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    url = None
    output = []
    lines = queue.Queue()
    threading.Thread(target=lambda: [lines.put(line) for line in process.stdout], daemon=True).start()
    deadline = time.time() + 120
    try:
        while time.time() < deadline:
            if process.poll() is not None and lines.empty():
                break
            try:
                line = lines.get(timeout=0.25)
            except queue.Empty:
                continue
            output.append(line)
            match = re.search(r"Viewer URL: (http://127\.0\.0\.1:\d+/[0-9a-f-]+/)", line)
            if match:
                url = match.group(1)
                break
        if not url:
            stderr = process.stderr.read() if process.poll() is not None else ""
            raise RuntimeError(f"viewer URL was not printed; stdout={''.join(output)!r}; stderr={stderr!r}")

        status, html, headers = fetch(url)
        if status != 200 or html != (embedded / "index.html").read_bytes():
            raise RuntimeError("release binary did not serve the exact committed production index")
        if headers.get("Cache-Control") != "no-store" or b"data-genohype-viewer-fallback" in html:
            raise RuntimeError("release binary served a fallback or cacheable index")
        references = re.findall(rb'(?:src|href)="\./assets/([^\"]+-[A-Za-z0-9_-]{8,}\.(?:js|css))"', html)
        if not references:
            raise RuntimeError("served production HTML has no checksum-addressed assets")
        application_bytes = bytearray()
        for reference in references:
            relative = reference.decode("ascii")
            status, asset, asset_headers = fetch(url + "assets/" + relative)
            expected = (embedded / "assets" / relative).read_bytes()
            if status != 200 or not asset or asset != expected:
                raise RuntimeError(f"embedded asset {relative} does not exactly match the committed file")
            if asset_headers.get("Cache-Control") != "public, max-age=31536000, immutable":
                raise RuntimeError(f"embedded checksum-addressed asset {relative} is not immutable")
            application_bytes.extend(asset)

        # Secondary Phase 5 capability markers: the exact-byte gate above is authoritative.
        for marker in (b"Loading association detail", b"Gene annotations unavailable", b"Ordered by locus"):
            if marker not in application_bytes:
                raise RuntimeError(f"embedded application lacks Phase 5 marker {marker!r}")

        manifest, manifest_body = checked_json(urllib.parse.urljoin(url, "api/manifest"), 16 * 1024 * 1024)
        dataset_id = manifest["plot_id"]
        if manifest.get("valid_row_count") != 10 or not manifest.get("resources", {}).get("table_api") or not manifest.get("resources", {}).get("region_api"):
            raise RuntimeError("release server root manifest is not an indexed Phase 5 dataset")

        for relative in ("chromosome/4", "locus/4/68000000-69000000"):
            status, deep_html, deep_headers = fetch(urllib.parse.urljoin(url, relative))
            expected_base = f'<base href="/{urllib.parse.urlparse(url).path.split("/")[1]}/">'.encode()
            if status != 200 or expected_base not in deep_html or deep_headers.get("Cache-Control") != "no-store":
                raise RuntimeError(f"release server did not serve the Phase 5 deep link {relative}")

        api_root = urllib.parse.urljoin(url, f"api/datasets/{dataset_id}/")
        table, table_body = checked_json(urllib.parse.urljoin(api_root, "table?limit=10"), 8 * 1024 * 1024)
        chromosome, chromosome_body = checked_json(urllib.parse.urljoin(api_root, "chromosomes/4/manifest"), 16 * 1024 * 1024)
        region_url = urllib.parse.urljoin(api_root, "region/manifest?contig=4&start=68000000&end=69000000&width=640&height=320&dpr=2")
        region, region_body = checked_json(region_url, 16 * 1024 * 1024)
        if table.get("total") != 10 or chromosome.get("layout", {}).get("contigs", [{}])[0].get("name") != "4":
            raise RuntimeError("indexed table or chromosome detail is unavailable")
        layout = region.get("layout", {})
        if (layout.get("requested_width"), layout.get("height"), layout.get("dpr")) != (640, 320, 2) \
                or layout.get("physical_width") != layout.get("width", 0) * 2 \
                or layout.get("physical_height") != 640:
            raise RuntimeError(f"region detail does not preserve logical and DPR physical geometry: {layout!r}")
        region_base = urllib.parse.urljoin(api_root, "region/")
        for descriptor in (region["resources"]["raster"], region["resources"]["overlay"]):
            status, body, response_headers = fetch(urllib.parse.urljoin(region_base, descriptor["url"]))
            if status != 200 or len(body) != descriptor["byte_size"]:
                raise RuntimeError("region detail resource identity is not retrievable")

        forbidden = [str(fixture), str(root), str(home), str(config), str(cache), str(association_cache)]
        dynamic = b"\n".join((manifest_body, table_body, chromosome_body, region_body)).decode("utf-8", "replace")
        if any(value in dynamic for value in forbidden):
            raise RuntimeError("dynamic release response disclosed a source or cache path")
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
    remaining_sessions = list((association_cache / "sessions").iterdir())
    if remaining_sessions:
        raise RuntimeError(f"ephemeral release generation was not deleted: {remaining_sessions}")
print("Phase 5 production association viewer embedding verified.")
PY
