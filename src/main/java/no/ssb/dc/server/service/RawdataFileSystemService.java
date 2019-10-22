package no.ssb.dc.server.service;

//            if (configuration.evaluateToBoolean("data.collector.rawdata.dump.enabled")) {
//                unwrap(RawdataFileSystemService.class).start();
//            }

public class RawdataFileSystemService  {// implements Service

//    private final DynamicConfiguration configuration;
//    private final RawdataClient rawdataClient;
//    private final AtomicBoolean running = new AtomicBoolean(false);
//    private RawdataFileSystemWriter writer;
//
//    public RawdataFileSystemService(DynamicConfiguration configuration, RawdataClient rawdataClient) {
//        this.configuration = configuration;
//        this.rawdataClient = rawdataClient;
//    }
//
//    @Override
//    public void start() {
//        if (configuration.evaluateToString("data.collector.rawdata.dump.location") == null) {
//            return;
//        }
//        if (!running.get()) {
//            running.set(true);
//            String location = configuration.evaluateToString("data.collector.rawdata.dump.location");
//            writer = new RawdataFileSystemWriter(
//                    rawdataClient,
//                    topic,
//                    location != null ?
//                            (location.startsWith("/") ? Paths.get(location).toAbsolutePath().normalize() : Paths.get(".").toAbsolutePath().resolve(location)).normalize() :
//                            Paths.get(".").toAbsolutePath().resolve("target").resolve("rawdata").normalize()
//            );
//            writer.start();
//        }
//    }
//
//    @Override
//    public void stop() {
//        if (running.get()) {
//            writer.shutdown();
//            running.set(false);
//        }
//    }
}
