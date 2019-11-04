module no.ssb.dc.server {

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.dc.api;
    requires no.ssb.dc.core;
    requires no.ssb.dc.application;
    requires no.ssb.dc.content.rawdata;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.avro;
    requires no.ssb.rawdata.kafka;

    requires org.slf4j;
    requires com.fasterxml.jackson.databind;
    requires jul_to_slf4j;
    requires ch.qos.logback.classic;
    requires ch.qos.logback.core;
    requires io.github.classgraph;

    requires undertow.core;

    exports no.ssb.dc.server;
    exports no.ssb.dc.server.controller;
    exports no.ssb.dc.server.service;
}
