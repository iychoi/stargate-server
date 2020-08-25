#! /bin/bash
if [ -z "${STARGATE_BIN}" ];
then
    echo -e "ERROR: Environment variable STARGATE_BIN is not defined"
    exit 1
else
    source ${STARGATE_BIN}/bootstrap.sh
fi

if [ -z "${TMUX_SESSION_NAME}" ];
then
    echo -e "ERROR: Environment variable TMUX_SESSION_NAME is not defined"
    exit 1
else
    tmux kill-session -t ${TMUX_SESSION_NAME}
    kill -9 $(ps aux | grep -e stargate-server | awk '{ print $2 }')
fi
