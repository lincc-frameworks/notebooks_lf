#!/bin/bash
source ~/.bashrc
conda activate cron
# Run script and write logs
python ~/jobs/tns.py >> ~/jobs/tns.log 2>&1
today_dir=$(date -u +"%Y-%m-%d")
# Re-arrange collections
mv /epyc/data3/hats/catalogs/tns/tmp/* /epyc/data3/hats/catalogs/tns
rm -r /epyc/data3/hats/catalogs/tns/tmp
# Register time of execution
echo "Ran at $(date -u)" >> tns.log
conda deactivate