#!/bin/bash
#SBATCH --job-name=load-dataset
#SBATCH --qos=np
#SBATCH --time=00:30:00
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=0
#SBATCH --output=/perm/cu0k/create-datasets/log/deltap-dataset.%j.out
#SBATCH --error=/perm/cu0k/create-datasets/log/deltap-dataset.%j.out

module load conda

# Following variables need to exported to the environment when submitting the job
# p_OUTPUT: path to the output zarr file
# p_LOG: path to the log file (for some basic bookkeeping)
# i_NPARTS: total number of parts
# i_PART:   current part number
# s_DELTA:  delta to calculate


echo "loading delta ${s_DELTA} part ${i_PART}/${i_NPARTS} for dataset ${p_OUTPUT}..."
sed -i "/${SLURM_JOB_ID}/s/submitted/running/" ${p_LOG}

conda run --live-stream -n anemoi-datasets-dev anemoi-datasets load-additions --trace ${p_OUTPUT} --part ${i_PART}/${i_NPARTS} --delta ${s_DELTA} 

if [ $? -ne 0 ];then 
  sed -i "/${SLURM_JOB_ID}/s/running/failed/" ${p_LOG}
  exit 1
else
  echo "done"
  rm -rf ${d_SCRATCH}
  sed -i "/${SLURM_JOB_ID}/s/running/finished/" ${p_LOG}
  exit 0
fi
