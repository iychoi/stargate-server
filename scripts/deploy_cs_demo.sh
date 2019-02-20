#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

work() {
    local node=$1
    scp -r ${RELDIR}/${RELEASE_ARCHIVE_FILENAME} ${node}:${ROOT_DIR}/
    ssh ${node} "rm -rf ${ROOT_DIR}/${RELEASE_NAME}" < /dev/null
    ssh ${node} "tar zxvf ${ROOT_DIR}/${RELEASE_ARCHIVE_FILENAME}" < /dev/null
    echo "Done ${node}"
}

while read node; do
    echo "connecting to ${node}"
    work ${node} &
done <cs_demo_cluster.txt
