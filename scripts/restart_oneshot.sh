#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"

source ${CUR_DIR}/stop_stargate_cs_demo.sh
source ${CUR_DIR}/stop_stargate_cs_hadoop.sh

source ${CUR_DIR}/make_release.sh
source ${CUR_DIR}/push_butler.sh

source ${CUR_DIR}/redeploy_cs_demo.sh
source ${CUR_DIR}/redeploy_cs_hadoop.sh

source ${CUR_DIR}/start_stargate_cs_demo.sh
source ${CUR_DIR}/start_stargate_cs_hadoop.sh
