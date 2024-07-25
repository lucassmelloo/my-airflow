
function getPDIJobFullLog {
    curl -s ""
}

PDI_JOB_STATUS=$(getPDIJobStatus)

while [ ${PDI_JOB_STATUS} = "Running" ]
do
    PDI_JOB_STATUS=$(getPDIJobStatus)
    echo "The PDI job status is: " ${PDI_JOB_STATUS}
    echo "I'll check in ${SLEEP_INTERVAL_SECONDS} seconds again"
    sleep ${SLEEP_INTERVAL_SECONDS}
done

echo "The PDI JOB status is: " ${PDI_JOB_STATUS}
echo "Printing full log ..."
echo ""
echo $(getPDIJobFullLog)