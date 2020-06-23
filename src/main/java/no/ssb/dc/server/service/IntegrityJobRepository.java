package no.ssb.dc.server.service;

public class IntegrityJobRepository {

    private final LmdbEnvironment lmdbEnvironment;

    public IntegrityJobRepository(LmdbEnvironment lmdbEnvironment) {
        this.lmdbEnvironment = lmdbEnvironment;

    }


}
