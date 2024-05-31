#!/bin/bash
#SBATCH --job-name=run_bench.ipynb
#SBATCH --partition=RM
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=48:00:00

module load anaconda3
conda activate ./cenv

python -m papermill run_bench.ipynb executed_run_bench.ipynb
