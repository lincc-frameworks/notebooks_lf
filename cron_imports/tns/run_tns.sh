#!/bin/bash
set -euo pipefail
source ~/.bashrc
conda activate cron
# Run script and write logs
python ~/jobs/tns/tns.py >> ~/jobs/tns/tns.log 2>&1
# Re-arrange collections
mv /epyc/data3/hats/catalogs/tns/tmp/* /epyc/data3/hats/catalogs/tns
rm -r /epyc/data3/hats/catalogs/tns/tmp
# Register time of execution
echo "Ran at $(date -u)" >> ~/jobs/tns/tns.log
conda deactivate