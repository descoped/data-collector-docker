FROM statisticsnorway/alpine-jdk13-buildtools:latest as build

#
# Build DataCollector Backend
#


#
# Build DataCollector image
#
FROM alpine:latest

RUN apk add --no-cache bash su-exec curl openssl

ENV DC_HOME=/opt/dc

ENV JAVA_OPTS=
ENV PROXY_HTTP_HOST=
ENV PROXY_HTTP_PORT=
ENV PROXY_HTTPS_HOST=
ENV PROXY_HTTPS_PORT=
ENV ENABLE_JMX_REMOTE_DEBUGGING=true

#ENV UID=dc
#ENV GID=dc

#RUN useradd -r --create-home --home-dir $DC_HOME --groups $GID --shell /bin/bash $UID

#
# Resources from build image
#
COPY --from=build /opt/jdk /opt/jdk/
COPY target/dependency $DC_HOME/lib/
RUN mkdir -p /ld_lib \
    && LMDB_NATIVE_JAR=$(find $DC_HOME/lib -type f -iname 'jffi-*-native.jar') \
    && unzip "$LMDB_NATIVE_JAR" "jni/x86_64-Linux/*" -d /ld_lib \
    && rm -f "$LMDB_NATIVE_JAR"
COPY target/data-collector-*.jar $DC_HOME/lib/
COPY target/classes/logback-stash.xml $DC_HOME/

ADD docker/start-collector.sh /run.sh
RUN chmod +x /run.sh

#COPY docker/entrypoint.bash /entrypoint.sh
#RUN chmod +x /entrypoint.sh

ENV PATH=/opt/jdk/bin:$PATH
ENV JAVA_HOME=/opt/jdk
ENV LD_LIBRARY_PATH=/ld_lib/jni/x86_64-Linux/:$LD_LIBRARY_PATH

WORKDIR $DC_HOME

VOLUME ["/conf", "/certs"]

EXPOSE 9990
EXPOSE 9992

#ENTRYPOINT ["/entrypoint.sh"]
CMD ["/run.sh"]
