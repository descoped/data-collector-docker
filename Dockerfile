FROM statisticsnorway/lds-server-base:latest as build

#
# Build DataCollector Backend
#
RUN ["jlink", "--strip-debug", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/opt/jdk/jmods", "--output", "/linked",\
 "--add-modules", "jdk.unsupported,java.base,java.management,java.net.http,java.xml,java.naming,java.sql,java.desktop,java.security.jgss,java.instrument,jdk.internal.vm.compiler"]
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
COPY --from=build /dc/server/target/data-collector-*.jar /opt/dc/lib/

ENV PATH=/opt/jdk/bin:$PATH

WORKDIR /opt/dc

VOLUME ["/conf", "/certs"]

EXPOSE 9990

CMD ["java", "-XX:+UnlockExperimentalVMOptions", "-XX:+EnableJVMCI", "-p", "/opt/dc/lib", "-m", "no.ssb.dc.server/no.ssb.dc.server.Server"]
