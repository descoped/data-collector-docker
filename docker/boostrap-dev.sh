#!/usr/bin/env bash
set -e

# test: PROXY_HTTP=localhost PROXY_PORT=10000 ./boostrap-dev.sh

if [ -n "$PROXY_HTTP" ]
then
  PROXY_OPTS="-Dhttp.proxyHost=$PROXY_HTTP"
fi

if [ -n "$PROXY_PORT" ]
then
  PROXY_OPTS="$PROXY_OPTS -Dhttp.proxyPort=$PROXY_PORT"
fi

echo "PROXY_OPTS=$PROXY_OPTS"

DEFAULT_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -Dcom.sun.management.jmxremote.rmi.port=9992 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9992 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost"

java $PROXY_OPTS $DEFAULT_OPTS -p /opt/dc/lib -m no.ssb.dc.server/no.ssb.dc.server.Server
