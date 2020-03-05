#!/usr/bin/env bash
set -e

# test: PROXY_HTTP_HOST=localhost PROXY_HTTP_PORT=10000 ./start-collector.sh

if [ -n "$PROXY_HTTP_HOST" ]
then
  PROXY_OPTS="-Dhttp.proxyHost=$PROXY_HTTP_HOST"
fi

if [ -n "$PROXY_HTTP_PORT" ]
then
  PROXY_OPTS="$PROXY_OPTS -Dhttp.proxyPort=$PROXY_HTTP_PORT"
fi

if [ -n "$PROXY_HTTPS_HOST" ]
then
  PROXY_OPTS="-Dhttps.proxyHost=$PROXY_HTTPS_HOST"
fi

if [ -n "$PROXY_HTTPS_PORT" ]
then
  PROXY_OPTS="$PROXY_OPTS -Dhttps.proxyPort=$PROXY_HTTPS_PORT"
fi

echo "PROXY_OPTS=$PROXY_OPTS"

DEFAULT_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -Dcom.sun.management.jmxremote.rmi.port=9992 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9992 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost"

java $PROXY_OPTS $DEFAULT_OPTS -p /opt/dc/lib -m no.ssb.dc.server/no.ssb.dc.server.Server