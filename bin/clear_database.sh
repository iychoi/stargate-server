#! /bin/bash
if [ -z "${STARGATE_BIN}" ];
then
    echo -e "ERROR: Environment variable STARGATE_BIN is not defined"
    exit 1
else
    source ${STARGATE_BIN}/bootstrap.sh
fi

if [ -z "${STARGATE_STORAGE}" ];
then
    echo -e "ERROR: Environment variable STARGATE_STORAGE is not defined"
    exit 1
else
    rm -rf ${STARGATE_STORAGE}/*
fi

if [ -z "${STARGATE_WORK}" ];
then
    echo -e "ERROR: Environment variable STARGATE_WORK is not defined"
    exit 1
else
    rm -rf ${STARGATE_WORK}/*
fi
