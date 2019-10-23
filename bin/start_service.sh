#! /bin/bash
if [ -z "${STARGATE_BIN}" ];
then
    echo -e "ERROR: Environment variable STARGATE_BIN is not defined"
    exit 1
else
    source ${STARGATE_BIN}/bootstrap.sh
fi

# VARIABLES PASSED TO THE STARGATE SERVICE
# - $STARGATE_STORAGE
# - $STARGATE_LIBS

if [ -z "${STARGATE_CONF}" ];
then
    echo -e "ERROR: Environment variable STARGATE_CONF is not defined"
    exit 1
fi

DEFAULT_SERVICE_CONFIG="${STARGATE_CONF}/service_config.json"

if [ -z "$1" ];
then
    SERVICE_CONFIG="${DEFAULT_SERVICE_CONFIG}"
else
    SERVICE_CONFIG="$1"
fi

java -cp "${STARGATE_LIBS}" ${STARGATE_MEMORY_PARAM} stargate.service.ServiceMain ${SERVICE_CONFIG}
