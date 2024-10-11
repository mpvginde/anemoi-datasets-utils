#!/bin/bash
#SBATCH --job-name=deltai-dataset
#SBATCH --qos=nf
#SBATCH --time=00:15:00
#SBATCH --nodes=1 
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output=/perm/cu0k/create-datasets/log/deltai-dataset.%j.out
#SBATCH --error=/perm/cu0k/create-datasets/log/deltai-dataset.%j.out

module load conda

# Helper function
dot_pad() {
  local text="$1"
  local total_length="40" # Desired total length of the output
  local current_length=${#text} # Length of the input string
  local pad_length=$((total_length - current_length)) # Calculate how much padding is needed

  if [ $pad_length -gt 0 ]; then
    # Create padding of dots for the remaining space
    printf "%s" "$text" # Print the original string
    printf "%.0s." $(seq 1 $pad_length) # Print dots for padding
  else
    echo -n "$text" # If the string is already longer than the desired length, return the string as is
  fi
}

# Following variables need to exported to the environment when submitting the job
# p_OUTPUT: path to the output zarr file
# p_LOG: path to the log file (for some basic bookkeeping)
# i_NPARTS: total number of parts
# s_DELTA: delta to calculate

echo "Initializing additional statistics for delta ${s_DELTA}..."
sed -i "/${SLURM_JOB_ID}/s/submitted/running/" ${p_LOG}

# 1. Initialisation
conda run --live-stream -n anemoi-datasets-dev anemoi-datasets init-additions ${p_OUTPUT} --delta ${s_DELTA}

if [ $? -ne 0 ];then 
  sed -i "/${SLURM_JOB_ID}/s/running/failed/" ${p_LOG}
  exit 1
else
  echo "done"
  sed -i "/${SLURM_JOB_ID}/s/running/finished/" ${p_LOG}

# 2. Load in parts
  jobids=""
  for i_PART in $(seq 1 1 ${i_NPARTS}); do
    echo "Submitting dataset delta load job ${i_PART}/${i_NPARTS}"
    
    jobid=$(sbatch --export=ALL,p_OUTPUT=${p_OUTPUT},i_PART=${i_PART},i_NPARTS=${i_NPARTS},p_LOG=${p_LOG},s_DELTA=${s_DELTA} \
      ${d_PROJECT}/create_dataset_delta_part.sh | awk '{print $4}')
    jobids="${jobids}:${jobid}"
    
    dot_pad "Loading delta ${s_DELTA} ${i_PART}/${i_NPARTS} (id: ${jobid})" >> ${p_LOG}
    
    printf "submitted\n" >> ${p_LOG}
  done

# 3. Finalisation
  echo "Submitting dataset finalisation job"

  jobid=$(sbatch --dependency=afterok${jobids} --export=ALL,p_OUTPUT=${p_OUTPUT},p_LOG=${p_LOG},f_DELTA=${s_DELTA} \
    ${d_PROJECT}/create_dataset_delta_final.sh | awk '{print $4}')

  dot_pad "Delta ${s_DELTA} finalisation (id: ${jobid})" >> ${p_LOG}
  printf "submitted\n" >> ${p_LOG}
  exit 0
fi
