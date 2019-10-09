package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.application.Service;

public class WorkerService implements Service {

    private final DynamicConfiguration configuration;

    public WorkerService(DynamicConfiguration configuration) {
        this.configuration = configuration;

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
