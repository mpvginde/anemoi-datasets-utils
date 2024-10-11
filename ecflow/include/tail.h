echo "sync ... `date`"
sync                      # flush all file I/O before signalling completion!
echo "wait ... `date`"
wait                      # wait for background process to stop
set +x
TASK_FINISH_TIME=`date -u +"%%Y-%%m-%%d %%H:%%M:%%S"`
echo "===== TASK STARTED:  ${TASK_START_TIME:-unknown} ====="
echo "===== TASK FINISHED: ${TASK_FINISH_TIME:-unknown} ====="
set -x
module reset
module load ecflow/%ECF_VERSION% || echo "no ecflow module found"

ecflow_client --complete  # Notify ecFlow of a normal end
trap 0                    # Remove all traps
exit 0                    # End the shell
