#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

REPO_HOST=butler.opencloud.cs.arizona.edu
REPO_ROOT_DIR=~/demo_apps/stargate

# copy to butler
scp -r ${RELLIBDIR}/stargate* ${REPO_HOST}:${REPO_ROOT_DIR}/libs/
