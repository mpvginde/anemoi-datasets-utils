#!/bin/bash
#SBATCH --job-name=final-dataset
#SBATCH --qos=nf
#SBATCH --time=00:10:00
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output=/perm/cu0k/create-datasets/log/final-dataset.%j.out
#SBATCH --error=/perm/cu0k/create-datasets/log/final-dataset.%j.out

module load conda

# Following variables need to exported to the environment when submitting the job
# p_OUTPUT: path to the output zarr file
# p_LOG: path to the log file (for some basic bookkeeping)

echo "Finalising dataset ${p_OUTPUT}..."
sed -i "/${SLURM_JOB_ID}/s/submitted/running/" ${p_LOG}

s_PROGRESS=$(conda run -n anemoi-datasets-dev anemoi-datasets inspect --progress ${p_OUTPUT}) 
if [ "$s_PROGRESS" == "Dataset not ready" ]; then 
  sed -i "/${SLURM_JOB_ID}/s/running/failed/" ${p_LOG}
  exit 1
else 
  conda run --live-stream -n anemoi-datasets-dev anemoi-datasets finalise ${p_OUTPUT}
  if [ $? -ne 0 ];then 
    sed -i "/${SLURM_JOB_ID}/s/running/failed/" ${p_LOG}
    exit 1
  else
    echo "done"
    sed -i "/${SLURM_JOB_ID}/s/running/finished/" ${p_LOG}
    exit 0
  fi
fi
