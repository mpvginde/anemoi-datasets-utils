module load conda
set -x
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

d_PROJECT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd ) #/perm/cu0k/anemoi-datasets-dev/my_tests
d_DATA=${d_PROJECT}/data
d_CONFIG=${d_PROJECT}/configs
d_LOG=${d_PROJECT}/log
i_NPARTS_LOAD=4
i_NPARTS_DELTA=2
l_DELTAS="3h 6h"

DEBUG=1

if [[ $DEBUG -eq 1 ]]; then
  f_CONFIG=cerra_2d_10u_10v_q_debug.yaml
  f_OUTPUT=debug.zarr
  f_LOG=debug.log
else
  f_CONFIG=cerra-rr-an-oper-0001-mars-5p5km-1984-1995-v1.yaml
  f_OUTPUT=cerra-rr-an-oper-0001-mars-5p5km-1984-1995-v1.zarr
  f_LOG=cerra-rr-an-oper-0001-mars-5p5km-1984-1995-v1.log
fi 

p_CONFIG=${d_CONFIG}/${f_CONFIG}
p_OUTPUT=${d_DATA}/${f_OUTPUT}
p_LOG=${d_LOG}/${f_LOG}

echo "Removing previous file ${f_OUTPUT}..."
rm -rf ${p_OUTPUT}
echo "done"


echo $(date +"%Y-%m-%d %H:%M:%S") > ${p_LOG}
echo "building: ${p_OUTPUT}" >> ${p_LOG}
echo "using config: ${p_CONFIG}" >> ${p_LOG}

# 1. Initialisation
dot_pad Initialisation >> ${p_LOG}
echo "Initializing anemoi-dataset from config ${f_CONFIG}..."

conda run --live-stream -n anemoi-datasets-dev anemoi-datasets init --trace --cache $SCRATCH ${p_CONFIG} ${p_OUTPUT}

if [ $? -ne 0 ];then 
  echo "failed" >> ${p_LOG}
  exit 1
else
  echo "finished" >> ${p_LOG}
  echo "done"

# 2. Loading in parts
  jobids=""
  for i_PART in $(seq 1 1 ${i_NPARTS_LOAD});do 
    echo "Submitting dataset load job ${i_PART}/${i_NPARTS_LOAD}"
    
    jobid=$(sbatch --export=ALL,p_OUTPUT=${p_OUTPUT},i_PART=${i_PART},i_NPARTS=${i_NPARTS_LOAD},p_LOG=${p_LOG} \
      ${d_PROJECT}/create_dataset_part.sh | awk '{print $4}')
    jobids="${jobids}:${jobid}"
    
    dot_pad "Loading ${i_PART}/${i_NPARTS_LOAD} (id: ${jobid})" >> ${p_LOG}
    printf "submitted\n" >> ${p_LOG}
  done

# 3. Finalisation
  echo "Submitting dataset finalisation job"

  jobid=$(sbatch --dependency=afterany${jobids} --export=ALL,p_OUTPUT=${p_OUTPUT},p_LOG=${p_LOG} \
      ${d_PROJECT}/create_dataset_final.sh | awk '{print $4}')

  dot_pad "Finalisation (id: ${jobid})" >> ${p_LOG}
  printf "submitted\n" >> ${p_LOG}

# 4. Additional Statistics
  jobid_final=${jobid}
  for s_DELTA in ${l_DELTAS}; do
    echo "Submitting dataset delta ${s_DELTA} job"

    jobid=$(sbatch --dependency=afterok:${jobid_final} --export=ALL,p_OUTPUT=${p_OUTPUT},i_NPARTS=${i_NPARTS_DELTA},p_LOG=${p_LOG},s_DELTA=${s_DELTA},d_PROJECT=${d_PROJECT},d_DATA=${d_DATA} \
      ${d_PROJECT}/create_dataset_delta.sh | awk '{print $4}')

    dot_pad "Delta ${s_DELTA} initialisation (id: ${jobid})" >> ${p_LOG}
    printf "submitted\n" >> ${p_LOG}
  done
  exit 0
fi