#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}/work/*" < /dev/null
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}/storage/*" < /dev/null
    ssh ${node} "rm -rf ~/stargate.log*" < /dev/null
    echo "Done ${node}"
}

while read node; do
    echo "connecting to ${node}"
    work ${node} &
done <cs_demo_cluster.txt
