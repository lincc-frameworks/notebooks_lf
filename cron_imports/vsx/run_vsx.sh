#!/bin/bash
set -euo pipefail
source ~/.bashrc
conda activate cron
mkdir -p /epyc/data3/hats/catalogs/vsx/tmp
# Run script and write logs
python ~/jobs/vsx/vsx.py >> ~/jobs/vsx/vsx.log 2>&1
# Re-arrange collections
mv /epyc/data3/hats/catalogs/vsx/tmp/$(date -u +"%Y-%m")/* /epyc/data3/hats/catalogs/vsx
rm -r /epyc/data3/hats/catalogs/vsx/tmp
# Register time of execution
echo "Ran at $(date -u)" >> ~/jobs/vsx/vsx.log
conda deactivate