#!/bin/bash
#SBATCH --job-name=%TASK%.%ECF_TRYNO%
#SBATCH --qos=%QOS:nf%
#SBATCH --time=%TIME:00:10:00%
#SBATCH --nodes=%NODES:1%
#SBATCH --ntasks-per-node=%NTASKS_PER_NODE:1%
#SBATCH --cpus-per-task=%CPUS_PER_TASK:1%
#SBATCH --account=bedb
#SBATCH --mem=%MEM:1000%