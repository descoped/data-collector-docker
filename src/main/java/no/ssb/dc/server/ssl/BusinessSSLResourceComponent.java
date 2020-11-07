package no.ssb.dc.server.ssl;

import no.ssb.dc.application.spi.Component;
import no.ssb.dc.application.ssl.BusinessSSLResourceSupplier;

public class BusinessSSLResourceComponent implements Component {

    private final BusinessSSLResourceSupplier businessSSLResourceSupplier;

    public BusinessSSLResourceComponent(BusinessSSLResourceSupplier businessSSLResourceSupplier) {
        this.businessSSLResourceSupplier = businessSSLResourceSupplier;
    }

    @Override
    public void initialize() {

    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public <R> R getDelegate() {
        return (R) businessSSLResourceSupplier;
    }

    @Override
    public void close() throws Exception {

    }
}
