#!/usr/bin/env bash
# Collect the seven KPL Hop summary files from complete per-Run result folders.
# Usage: bash kpl_pack_delay_hop_7files.sh [input_dir] [output.tar.gz]
set -euo pipefail

SOURCE_DIR="$(realpath -e "${1:-./kpl-delay-hop-analysis}")"
ARCHIVE="$(realpath -m "${2:-./kpl-delay-hop-7files.tar.gz}")"

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "ERROR: Not a directory: $SOURCE_DIR" >&2
  exit 1
fi

required=(
  manifest.json
  hop_quality.csv
  hop_timing.csv
  message_summary.csv
  path_status_summary.csv
  placement_timing.csv
  session_quality.csv
)

listfile="$(mktemp)"
trap 'rm -f "$listfile"' EXIT
complete=0
incomplete=0

while IFS= read -r -d '' manifest; do
  run_dir="${manifest%/*}"
  missing=()
  for file in "${required[@]}"; do
    if [[ ! -f "$run_dir/$file" ]]; then
      missing+=("$file")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    printf 'SKIP (incomplete): %s (missing: %s)\n' "${run_dir#"$SOURCE_DIR"/}" "${missing[*]}" >&2
    ((incomplete+=1))
    continue
  fi
  rel="${run_dir#"$SOURCE_DIR"/}"
  for file in "${required[@]}"; do
    printf '%s\0' "$rel/$file" >> "$listfile"
  done
  ((complete+=1))
done < <(find "$SOURCE_DIR" -type f -name manifest.json -print0 | sort -z)

if (( complete == 0 )); then
  echo "ERROR: No complete Run directories with all seven files found under: $SOURCE_DIR" >&2
  exit 1
fi

mkdir -p "$(dirname "$ARCHIVE")"
# GNU tar: --null allows safe filenames, -C ensures relative paths in archive.
tar -C "$SOURCE_DIR" --null -czf "$ARCHIVE" -T "$listfile"

echo "Complete runs archived: $complete"
echo "Incomplete runs skipped: $incomplete"
echo "Files included: $((complete * 7))"
echo "Archive: $ARCHIVE"
ls -lh "$ARCHIVE"
