module no.ssb.dc.server {

    requires jdk.unsupported;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.dc.api;
    requires no.ssb.dc.core;
    requires no.ssb.dc.application;
    requires no.ssb.dc.content.rawdata;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.avro;
    requires dapla.secrets.client.api;
    requires dapla.secrets.provider.safe.configuration;
    requires dapla.secrets.provider.dynamic.configuration;
    requires dapla.secrets.provider.google.rest.api;

    requires java.instrument;

    requires net.bytebuddy;
    requires net.bytebuddy.agent;
    requires org.slf4j;
    requires com.fasterxml.jackson.databind;
    requires jul.to.slf4j;
    requires ch.qos.logback.classic;
    requires ch.qos.logback.core;
    requires logstash.logback.encoder;
    requires io.github.classgraph;
    requires org.apache.tika.core;

    requires java.net.http;
    requires undertow.core;

    requires org.lmdbjava;
    requires org.objectweb.asm;

    opens no.ssb.dc.server;
    opens worker.config;

    opens no.ssb.dc.server.content to com.fasterxml.jackson.databind, org.apache.tika.core;
    opens no.ssb.dc.server.task to com.fasterxml.jackson.databind;
    opens no.ssb.dc.server.integrity to com.fasterxml.jackson.databind;
    opens no.ssb.dc.server.recovery to com.fasterxml.jackson.databind;

    exports no.ssb.dc.server;
    exports no.ssb.dc.server.content;
    exports no.ssb.dc.server.db;
    exports no.ssb.dc.server.integrity;
    exports no.ssb.dc.server.recovery;
    exports no.ssb.dc.server.ssl;
    exports no.ssb.dc.server.task;
}
