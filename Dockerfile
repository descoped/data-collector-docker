FROM statisticsnorway/lds-server-base:latest as build

#
# Build DataCollector Backend
#
RUN ["jlink", "--strip-debug", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/opt/jdk/jmods", "--output", "/linked",\
 "--add-modules", "jdk.unsupported,java.base,java.management,java.net.http,java.xml,java.naming,java.sql,java.desktop,java.security.jgss,java.instrument"]
COPY pom.xml /dc/server/
WORKDIR /dc/server
RUN mvn -B verify dependency:go-offline
COPY src /dc/server/src/
RUN mvn -B verify && mvn -B dependency:copy-dependencies

#
# Build DataCollector image
#
FROM alpine:latest

#
# Resources from build image
#
COPY --from=build /linked /opt/jdk/
COPY --from=build /dc/server/target/dependency /opt/dc/lib/
COPY --from=build /dc/server/target/data-collector-*.jar /opt/dc/server/

ENV PATH=/opt/jdk/bin:$PATH

WORKDIR /opt/dc

VOLUME ["/conf", "/certs", "/rawdata"]

EXPOSE 9090

CMD ["java", "--illegal-access=deny", "-cp", "/opt/dc/server/*:/opt/dc/lib/*", "no.ssb.dc.ske.freg.Server"]
