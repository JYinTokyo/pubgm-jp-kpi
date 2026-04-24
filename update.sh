#!/bin/bash
cd /Users/kim_yeong/pubgm-jp-kpi
source .env

echo "$(date): Generating dashboard..."
python3 generate.py

git add index.html
git diff --staged --quiet || (git commit -m "Update dashboard $(date +'%Y-%m-%d')" && git push)
echo "$(date): Done."
