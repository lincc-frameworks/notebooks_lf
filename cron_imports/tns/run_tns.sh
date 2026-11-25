#!/bin/bash
set -euo pipefail
source ~/.bashrc
conda activate cron
# Run script and write logs
python ~/jobs/tns/tns.py >> ~/jobs/tns/tns.log 2>&1
# Re-arrange collections
mv /astro/store/shire/hats/catalogs/tns/tmp/* /astro/store/shire/hats/catalogs/tns
rm -r /astro/store/shire/hats/catalogs/tns/tmp
# Register time of execution
echo "Ran at $(date -u)" >> ~/jobs/tns/tns.log
conda deactivate