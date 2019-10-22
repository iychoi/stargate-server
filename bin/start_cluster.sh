#! /bin/bash
if [ -z "${STARGATE_BIN}" ];
then
    echo -e "ERROR: Environment variable STARGATE_BIN is not defined"
    exit 1
else
    source ${STARGATE_BIN}/bootstrap.sh
fi

work() {
    local node=$1
    ssh ${node} "/bin/bash --login -c ${STARGATE_BIN}/clear_database.sh" < /dev/null
    ssh ${node} "/bin/bash --login -c ${STARGATE_BIN}/start_service_tmux.sh" < /dev/null
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
