module no.ssb.dc.server {

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.dc.api;
    requires no.ssb.dc.application;
    requires no.ssb.dc.core;
    requires no.ssb.dc.content.rawdata;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.pulsar;
    //requires no.ssb.rawdata.kafka;

    requires org.slf4j;
    requires io.github.classgraph;

    requires undertow.core;

    exports no.ssb.dc.server;
}
