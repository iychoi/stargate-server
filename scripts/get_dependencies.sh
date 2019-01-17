#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

SC_FILE="${DEPDIR}/stargate-commons-1.0.jar"

if [ -f "${SC_FILE}" ]
then
    rm ${SC_FILE}
fi

mvn -f ${BASEDIR}/pom.xml install dependency:copy-dependencies
