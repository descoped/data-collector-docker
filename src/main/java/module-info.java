module no.ssb.dc.server {

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.dc.api;
    requires no.ssb.rawdata.api;

    requires org.slf4j;
    requires io.github.classgraph;

    requires undertow.core;

    exports no.ssb.dc.server;
    exports no.ssb.dc.server.service;
    exports no.ssb.dc.server.controller;
}
