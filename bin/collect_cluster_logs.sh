#! /bin/bash
if [ -z "${STARGATE_BIN}" ];
then
    echo -e "ERROR: Environment variable STARGATE_BIN is not defined"
    exit 1
else
    source ${STARGATE_BIN}/bootstrap.sh
fi

CLUSETER_LOG_DIR=logs
mkdir -p ${CLUSETER_LOG_DIR}

work() {
    local node=$1
    scp ${node}:${STARGATE_LOG}/stargate.log ${CLUSETER_LOG_DIR}/stargate.${node}.log
    echo "Done ${node}"
}

if [ -z "${STARGATE_CONF}" ];
then
    echo -e "ERROR: Environment variable STARGATE_CONF is not defined"
    exit 1
fi

while read node; do
    echo "connecting to ${node}"
    work ${node}
done <${STARGATE_CONF}/cluster

echo "All jobs completed"
