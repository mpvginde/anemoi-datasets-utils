#!/bin/bash
#SBATCH --job-name=deltaf-dataset
#SBATCH --qos=nf
#SBATCH --time=00:10:00
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output=/perm/cu0k/create-datasets/log/deltaf-dataset.%j.out
#SBATCH --error=/perm/cu0k/create-datasets/log/deltaf-dataset.%j.out

module load conda

# Following variables need to exported to the environment when submitting the job
# p_OUTPUT: path to the output zarr file
# p_LOG: path to the log file (for some basic bookkeeping)
# s_DELTA: the delta that is being calculated 


echo "Finalising delta ${s_DELTA} for dataset ${p_OUTPUT}..."
sed -i "/${SLURM_JOB_ID}/s/submitted/running/" ${p_LOG}

conda run --live-stream -n anemoi-datasets-dev anemoi-datasets finalise-additions ${p_OUTPUT} --delta ${s_DELTA}
if [ $? -ne 0 ];then 
  sed -i "/${SLURM_JOB_ID}/s/running/failed/" ${p_LOG}
  exit 1
else
  echo "done"
  sed -i "/${SLURM_JOB_ID}/s/running/finished/" ${p_LOG}
  exit 0
fi
