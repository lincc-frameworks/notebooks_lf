#!/bin/bash
source ~/.bashrc
conda activate cron
cd /astro/users/smcampos/jobs
python tns.py >> tns.log 2>&1
echo "Ran at $(date -u)" >> tns.log
conda deactivate
