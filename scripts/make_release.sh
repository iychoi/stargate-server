#! /bin/bash
CUR_DIR="$(dirname $(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}")))"
source ${CUR_DIR}/bootstrap.sh

# prepare an empty directory for release
if [[ ! -d "${RELDIR}" ]];
then
  #echo "a directory "$RELDIR does not exist"
  mkdir -p ${RELDIR}
else
  #echo "a directory "$RELDIR exists"
  rm -rf ${RELDIR}/*
fi

# get dependencies
SC_FILE="${DEPDIR}/stargate-commons-1.0.jar"

if [ -f "${SC_FILE}" ]
then
    rm ${SC_FILE}
fi
mvn -f ${BASEDIR}/pom.xml install dependency:copy-dependencies

# build package
mvn -f ${BASEDIR}/pom.xml package

# copy bin files
mkdir -p ${RELBINDIR}
cp ${BINDIR}/* ${RELBINDIR}/

# replace bootstrap.sh
LIBS_LINE="DEFAULT_STARGATE_LIBS=\"\${BASEDIR}/libs/*\""
sed -i 's|^DEFAULT_STARGATE_LIBS.*|'"${LIBS_LINE}"'|g' ${RELBINDIR}/bootstrap.sh


# copy jar files
mkdir -p ${RELLIBDIR}
cp ${JARDIR}/stargate*.jar ${RELLIBDIR}/
cp ${DEPDIR}/* ${RELLIBDIR}/

# make a storage directory
mkdir -p ${RELSTORAGEDIR}

# copy License and Readme
cp ${BASEDIR}/LICENSE ${RELDIR}/
cp ${BASEDIR}/README.md ${RELDIR}/README.md
