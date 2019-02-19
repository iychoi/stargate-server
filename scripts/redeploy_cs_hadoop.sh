#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    scp ${RELLIBDIR}/stargate* ${node}:${ROOT_DIR}/${RELEASE_NAME}/libs
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}/work/*" < /dev/null
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}/storage/*" < /dev/null
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}/bin/stargate.log*" < /dev/null
    echo "Done ${node}"
}

while read node; do
    echo "connecting to ${node}"
    work ${node} &
done <cs_hadoop_cluster.txt
