#! /bin/bash
if [ -z "${STARGATE_CONF}" ];
then
    echo -e "ERROR: Environment variable STARGATE_CONF is not defined"
    exit 1
else
    source ${STARGATE_CONF}/env.sh
fi
