#! /bin/bash
# current directory
SCRIPTDIR=$(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}"))
BASEDIR=$(dirname $(dirname ${SCRIPTDIR}))
BINDIR="${BASEDIR}/bin"
JARDIR="${BASEDIR}/target"
DEPDIR="${BASEDIR}/target/dependency"

RELDIR="${BASEDIR}/release"
RELBINDIR="${RELDIR}/bin"
RELLIBDIR="${RELDIR}/libs"
RELSTORAGEDIR="${RELDIR}/storage"
