#!/bin/bash
set -e
mkdir -p /app/data/recordings
exec python3 -u /app/main.py
