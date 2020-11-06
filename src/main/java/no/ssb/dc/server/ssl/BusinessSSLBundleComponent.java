package no.ssb.dc.server.ssl;

import no.ssb.dc.application.spi.Component;
import no.ssb.dc.application.ssl.BusinessSSLBundleSupplier;

import java.util.Objects;

public class BusinessSSLBundleComponent implements Component {

    private final BusinessSSLBundleSupplier businessSSLBundleSupplier;

    public BusinessSSLBundleComponent(BusinessSSLBundleSupplier businessSSLBundleSupplier) {
        this.businessSSLBundleSupplier = businessSSLBundleSupplier;
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
        Objects.requireNonNull(businessSSLBundleSupplier);
        return (R) businessSSLBundleSupplier;
    }

    @Override
    public void close() throws Exception {

    }
}
