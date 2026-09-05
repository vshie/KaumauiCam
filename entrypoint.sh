#!/bin/bash
set -e
mkdir -p /app/data/recordings /app/data/stereo
exec python3 -u /app/main.py
