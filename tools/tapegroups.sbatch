#!/bin/bash
#SBATCH --job-name=tapegroups
#SBATCH --qos=nf
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=4
#SBATCH --account=bedb
# Run a tape-map tool as a batch job (MARS probing: not on the login node).
# Usage: sbatch --output=<outdir>/%x.%j.out tools/tapegroups.sbatch <absolute path of tool.py> <args...>
# (absolute path: sbatch runs a spooled copy of this script, so $0 is not in tools/)
set -e
module load python3
date
python3 "$@"
date
