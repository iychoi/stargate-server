#! /bin/bash
# current directory
SCRIPTDIR=$(realpath $(test -L "${BASH_SOURCE}" && readlink -f "${BASH_SOURCE}" || echo "${BASH_SOURCE}"))
BASEDIR=$(dirname $(dirname ${SCRIPTDIR}))
BINDIR="${BASEDIR}/bin"
CONFDIR="${BASEDIR}/config"
JARDIR="${BASEDIR}/target"
DEPDIR="${BASEDIR}/target/dependency"

RELDIR="${BASEDIR}/release"
RELBINDIR="${RELDIR}/bin"
RELCONFDIR="${RELDIR}/config"
RELLIBDIR="${RELDIR}/libs"
RELSTORAGEDIR="${RELDIR}/storage"

RELEASE_NAME=stargate-release-1.0
RELEASE_ARCHIVE_FILENAME=${RELEASE_NAME}.tar.gz

TMUX_SESSION_NAME=stargate
RELEASE_REPO_URL=https://butler.opencloud.cs.arizona.edu/demo_apps/stargate
