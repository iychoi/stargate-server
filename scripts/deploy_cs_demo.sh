#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}" < /dev/null
    ssh ${node} "rm -rf ~/stargate.log*" < /dev/null
    ssh ${node} "wget --no-check-certificate -N ${RELEASE_REPO_URL}/${RELEASE_ARCHIVE_FILENAME}" < /dev/null
    ssh ${node} "tar zxvf ${ROOT_DIR}/${RELEASE_ARCHIVE_FILENAME}" < /dev/null
    echo "Done ${node}"
}

PIDS=()
while read node; do
    echo "connecting to ${node}"
    work ${node} &
    PIDS+=($!)
done <cs_demo_cluster.txt

for pid in ${PIDS[*]}; do
    wait $pid
done

echo "All jobs completed"
