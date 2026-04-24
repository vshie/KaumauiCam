#!/usr/bin/env bash
# Build linux/arm64 image locally, write kaumaui_cam.tar, then git push (no commit).
# Commit your changes first. The .tar is gitignored.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

IMAGE="${IMAGE:-vshie/kaumaui_cam:dev}"
OUT="${OUT:-kaumaui_cam.tar}"

echo "==> docker buildx build --platform linux/arm64 -t $IMAGE --load ."
docker buildx build --platform linux/arm64 -t "$IMAGE" --load .

echo "==> docker save -> $OUT"
docker save "$IMAGE" -o "$OUT"
ls -lh "$OUT"

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "==> git push origin $BRANCH"
git push origin "$BRANCH"

echo "Done."
