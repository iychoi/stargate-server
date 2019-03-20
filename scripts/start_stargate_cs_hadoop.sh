#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    ssh ${node} "rm -rf ~/stargate.log*" < /dev/null
    ssh ${node} "tmux new-session -d -s '${TMUX_SESSION_NAME}' '${ROOT_DIR}/${RELEASE_NAME}/bin/start_service.sh ${ROOT_DIR}/${RELEASE_NAME}/bin/service_config_cs_hadoop.json'" < /dev/null
    echo "Done ${node}"
}

PIDS=()
while read node; do
    echo "connecting to ${node}"
    work ${node} &
    PIDS+=($!)
done <cs_hadoop_cluster.txt

for pid in ${PIDS[*]}; do
    wait $pid
done

echo "All jobs completed"
