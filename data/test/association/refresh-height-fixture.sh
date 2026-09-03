#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
GENOHYPE_BIN=${GENOHYPE_BIN:-"$REPO_ROOT/target/debug/genohype"}
OUTPUT="${TMPDIR:-/tmp}/genohype-association-real-data/height-chr4-68000000-69000000.ht"
ALLOW_TRACKED_OUTPUT=0
SOURCE="gs://aou_results/414k/ht_results/META/phenotype_height/genome_variant_results.ht"
INTERVAL="chr4:68000000-69000000"

while (($#)); do
  case "$1" in
    --output)
      [[ $# -ge 2 ]] || { echo "--output requires a path" >&2; exit 2; }
      OUTPUT=$2
      shift 2
      ;;
    --allow-tracked-output)
      ALLOW_TRACKED_OUTPUT=1
      shift
      ;;
    -h|--help)
      cat <<EOF
Usage: $0 [--output PATH] [--allow-tracked-output]

Exports the AoU height chr4 interval to an untracked temporary path by default.
Writing anywhere inside the Genohype repository requires --allow-tracked-output.
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

OUTPUT=$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$OUTPUT")
case "$OUTPUT" in
  ""|"/"|"$HOME"|"$REPO_ROOT")
    echo "refusing unsafe output path: $OUTPUT" >&2
    exit 2
    ;;
  *.ht) ;;
  *)
    echo "output must be a dedicated .ht directory: $OUTPUT" >&2
    exit 2
    ;;
esac
case "$OUTPUT/" in
  "$REPO_ROOT"/*)
    if [[ $ALLOW_TRACKED_OUTPUT -ne 1 ]]; then
      echo "refusing to write inside $REPO_ROOT without --allow-tracked-output" >&2
      exit 2
    fi
    ;;
esac

[[ -x "$GENOHYPE_BIN" ]] || {
  echo "Genohype binary not found at $GENOHYPE_BIN; run cargo build or set GENOHYPE_BIN" >&2
  exit 2
}

mkdir -p "$(dirname "$OUTPUT")"
rm -rf "$OUTPUT"
"$GENOHYPE_BIN" export hail "$SOURCE" "$OUTPUT" --interval "$INTERVAL"
printf 'Wrote optional real-data fixture to %s\n' "$OUTPUT"
