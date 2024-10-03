module io.descoped.dc.server {

    requires jdk.unsupported;

    requires io.descoped.service.provider.api;
    requires io.descoped.dynamic.config;
    requires io.descoped.dc.api;
    requires io.descoped.dc.core;
    requires io.descoped.dc.application;
    requires io.descoped.dc.content.rawdata;
    requires io.descoped.rawdata.api;
    requires io.descoped.rawdata.postgres;
    requires io.descoped.rawdata.avro;
    requires io.descoped.secrets.client.api;
    requires io.descoped.secrets.provider.safe.configuration;
    requires io.descoped.secrets.provider.dynamic.configuration;
    requires io.descoped.secrets.provider.google.rest.api;

    requires java.instrument;

    requires net.bytebuddy;
    requires net.bytebuddy.agent;
    requires org.slf4j;
    requires jul.to.slf4j;
    requires com.fasterxml.jackson.databind;
    requires io.github.classgraph;
    requires org.apache.tika.core;

    requires java.net.http;
    requires undertow.core;

    requires org.lmdbjava;
    requires org.objectweb.asm;

    opens io.descoped.dc.server;
    opens worker.config;

    opens io.descoped.dc.server.content to com.fasterxml.jackson.databind, org.apache.tika.core;
    opens io.descoped.dc.server.task to com.fasterxml.jackson.databind;
    opens io.descoped.dc.server.integrity to com.fasterxml.jackson.databind;
    opens io.descoped.dc.server.recovery to com.fasterxml.jackson.databind;

    exports io.descoped.dc.server;
    exports io.descoped.dc.server.content;
    exports io.descoped.dc.server.db;
    exports io.descoped.dc.server.integrity;
    exports io.descoped.dc.server.recovery;
    exports io.descoped.dc.server.ssl;
    exports io.descoped.dc.server.task;
}
