#! /bin/bash
SCRIPTDIR=$(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}"))
BASEDIR=$(dirname $(dirname ${SCRIPTDIR}))

DEFAULT_STARGATE_STORAGE="${BASEDIR}/storage"
STARGATE_STORAGE="${STARGATE_STORAGE:-${DEFAULT_STARGATE_STORAGE}}"

DEFAULT_STARGATE_LIBS="${BASEDIR}/libs/*:${BASEDIR}/target/stargate-server-1.0.jar:${BASEDIR}/target/dependency/*"
STARGATE_LIBS="${STARGATE_LIBS:-${DEFAULT_STARGATE_LIBS}}"
