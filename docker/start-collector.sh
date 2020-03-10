#!/usr/bin/env bash
set -e

JPMS_SWITCHES="
  --add-reads no.ssb.dc.core=ALL-UNNAMED
  --add-opens no.ssb.dc.core/no.ssb.dc.core=ALL-UNNAMED
  --add-reads no.ssb.dc.content.rawdata=no.ssb.dc.core
"

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

if [ -n "$PROXY_OPTS" ]
then
  echo "PROXY_OPTS=$PROXY_OPTS"
fi

DEFAULT_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -Dcom.sun.management.jmxremote.rmi.port=9992 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9992 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost"

BYTE_BUDDY_AGENT_JAR=$(find /opt/dc/lib/ -type f -iname 'byte-buddy-agent*')

java $JPMS_SWITCHES $PROXY_OPTS $DEFAULT_OPTS -XX:+StartAttachListener -javaagent:$BYTE_BUDDY_AGENT_JAR -p /opt/dc/lib -m no.ssb.dc.server/no.ssb.dc.server.Server
