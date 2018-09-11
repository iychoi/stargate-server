#! /bin/bash
SC_FILE="target/dependency/stargate-commons-1.0.jar"

if [ -f "${SC_FILE}" ]
then
    rm ${SC_FILE}
fi

mvn install dependency:copy-dependencies
