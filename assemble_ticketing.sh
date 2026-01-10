#!/usr/bin/env bash
set -euo pipefail

OUT="/home/tmthy/Vibey/cogs/ticketing.py"
TMP="/home/tmthy/Vibey/ticketing.b64"
mkdir -p "$(dirname "$OUT")"
: > "$TMP"

append_chunk() {
  cat >> "$TMP"
  echo >> "$TMP"
}

finalize() {
  base64 -d "$TMP" > "$OUT"
  chmod 644 "$OUT"
  echo "Wrote $OUT"
  sha256sum "$OUT"
}

echo "Assembler loaded into your shell."
echo
echo "Paste chunks like this:"
echo "append_chunk <<'B64'"
echo "<PASTE-CHUNK-HERE>"
echo "B64"
echo
echo "When done, run:  finalize"
