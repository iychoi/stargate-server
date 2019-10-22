#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

REPO_HOST=lectura.cs.arizona.edu
REPO_ROOT_DIR=~/webpage/app/stargate

# copy to butler
scp -r ${RELDIR}/${RELEASE_ARCHIVE_FILENAME} ${REPO_HOST}:${REPO_ROOT_DIR}/
scp -r ${RELLIBDIR}/stargate*.jar ${REPO_HOST}:${REPO_ROOT_DIR}/libs/
