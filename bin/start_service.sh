#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh


# VARIABLES PASSED TO THE STARGATE SERVICE
# - $STARGATE_STORAGE
# - $STARGATE_LIBS

java -cp "${STARGATE_LIBS}" stargate.service.ServiceMain $@
