#!/usr/bin/env bash
# Migrate series repos from chapters/ subdirectory to flat layout.
#
# Usage:
#   ./migrate-flat-chapters.sh /path/to/pijul-store
#
# For fedi-xanadu:  ./migrate-flat-chapters.sh /var/lib/fedi-xanadu/pijul-store
# For pijul-pad:    ./migrate-flat-chapters.sh /var/lib/pijul-pad/repos
#
# What it does for each repo that has a chapters/ directory:
#   1. Moves all files from chapters/ to the repo root
#   2. Removes the empty chapters/ directory
#   3. Skips repos without a chapters/ dir (already flat or non-series)
#
# Safe to run multiple times (idempotent).

set -euo pipefail

STORE_DIR="${1:?Usage: $0 /path/to/pijul-store}"

if [ ! -d "$STORE_DIR" ]; then
    echo "Error: $STORE_DIR does not exist"
    exit 1
fi

migrated=0
skipped=0

for repo in "$STORE_DIR"/*/; do
    chapters_dir="$repo/chapters"
    if [ ! -d "$chapters_dir" ]; then
        skipped=$((skipped + 1))
        continue
    fi

    # Check if chapters/ has any files
    file_count=$(find "$chapters_dir" -maxdepth 1 -type f | wc -l)
    if [ "$file_count" -eq 0 ]; then
        # Empty chapters dir, just remove it
        rmdir "$chapters_dir" 2>/dev/null || true
        echo "  removed empty chapters/ in $(basename "$repo")"
        migrated=$((migrated + 1))
        continue
    fi

    # Move all files from chapters/ to repo root
    for f in "$chapters_dir"/*; do
        [ -e "$f" ] || continue
        dest="$repo/$(basename "$f")"
        if [ -e "$dest" ]; then
            echo "  WARN: $dest already exists, skipping $(basename "$f") in $(basename "$repo")"
            continue
        fi
        mv "$f" "$dest"
    done

    # Remove chapters/ if empty now
    rmdir "$chapters_dir" 2>/dev/null || echo "  WARN: chapters/ not empty in $(basename "$repo")"

    echo "  migrated $(basename "$repo") ($file_count files)"
    migrated=$((migrated + 1))
done

echo ""
echo "Done: $migrated repos migrated, $skipped skipped (no chapters/ dir)"
