#!/bin/bash
set -e
mkdir -p /app/data/recordings /app/data/in_progress
exec python3 -u /app/main.py
