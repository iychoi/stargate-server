#! /bin/bash
java -cp "target/stargate-server-1.0.jar:target/dependency/*" stargate.admin.cli.DataSources $@
