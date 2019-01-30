#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

ROOT_DIR=~

#INPUT=$@
INPUT=service_config*.json

work() {
    local node=$1
    scp ${INPUT} ${node}:${ROOT_DIR}/${RELEASE_NAME}/bin/
}

while read node; do
    echo "connecting to ${node}"
    work ${node} &
done <cs_hadoop_cluster.txt
