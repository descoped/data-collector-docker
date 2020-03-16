#!/usr/bin/env bash

mvn clean verify dependency:copy-dependencies -DskipTests

cp target/data-collector-docker-0.1-SNAPSHOT.jar target/dependency

JAR_PATH="target/dependency"
BYTE_BUDDY_AGENT_JAR=$(find ~+/$JAR_PATH -type f -iname 'byte-buddy-agent*')

java -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -Dcom.sun.management.jmxremote.rmi.port=9992 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9992 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost -XX:+StartAttachListener -javaagent:$BYTE_BUDDY_AGENT_JAR -p $PWD/target/dependency -m no.ssb.dc.server/no.ssb.dc.server.Server
