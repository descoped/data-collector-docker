package no.ssb.dc.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.test.client.ResponseHelper;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.controller.EventItem;
import no.ssb.dc.test.server.TestServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.regex;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

/**
 * The test scope is to produce >8Mb of data written to local-temp and trigger file upload to GCS.
 * During upload the Worker should receive endpoint failure >3 times to trigger shutdown during upload process.
 * <p>
 * The test should verify a successful upload before graceful shutdown of worker.
 * <p>
 * Pre-requisite:
 * - Copy your SA-account file to ./certs/my_gcs_sa.json (gitignored)
 */

public class GCSUploadFailureTest {

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .values("content.stream.connector", "rawdata")

//            .values("rawdata.client.provider", "filesystem")
            .values("avro-file.sync.interval", Long.toString(200))
            .values("filesystem.storage-folder", "target/avro_store")
            .values("listing.min-interval-seconds", "3")

            .values("rawdata.client.provider", "gcs")
            .values("local-temp-folder", "target/avro_temp")
            .values("avro-file.max.seconds", "3")
            .values("avro-file.max.bytes", Long.toString(64 * 1024 * 1024)) // 64 Mb
            .values("gcs.bucket-name", "test-gcs-provider")
            .values("gcs.listing.min-interval-seconds", "3")
            .values("gcs.credential-provider", "service-account")
            .values("gcs.service-account.key-file", "certs/my_gcs_sa.json")
            .build();

    static TestClient client;

    static TestServer server;

    @BeforeAll
    static void before() {
        server = TestServer.create(configuration);
        client = TestClient.create(server);
        server.start();
    }

    @AfterAll
    static void after() {
        server.stop();
    }

    static GoogleCredentials getWriteCredentials() throws IOException {
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(Path.of(configuration.evaluateToString("gcs.service-account.key-file")), StandardOpenOption.READ));
        return credentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
    }

    static void purgeTempFilesAndBucket() throws IOException {
        Path avroTemp = CommonUtils.currentPath().resolve("target/avro_temp");
        Path avroStore = CommonUtils.currentPath().resolve("target/avro_store");
        if (avroTemp.toFile().exists())
            Files.walk(avroTemp).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        if (avroStore.toFile().exists())
            Files.walk(avroStore).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);

        GoogleCredentials scopedCredentials = getWriteCredentials();
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        Page<Blob> page = storage.list(configuration.evaluateToString("gcs.bucket-name"), Storage.BlobListOption.prefix("topic"));
        BlobId[] blobs = StreamSupport.stream(page.iterateAll().spliterator(), false).map(BlobInfo::getBlobId).collect(Collectors.toList()).toArray(new BlobId[0]);
        if (blobs.length > 0) {
            List<Boolean> deletedList = storage.delete(blobs);
            for (Boolean deleted : deletedList) {
                if (!deleted) {
                    throw new RuntimeException("Unable to delete blob in bucket");
                }
            }
        }
    }

    static List<BucketObject> longListTempFiles() throws IOException {
        List<BucketObject> files = new ArrayList<>();
        Path avroTemp = CommonUtils.currentPath().resolve("target/avro_temp");
        Files.walk(avroTemp).sorted(Comparator.naturalOrder()).map(Path::toFile).forEach(file -> {
            files.add(new BucketObject(file.length(), file.getName(), file.isDirectory(), 0L, file.lastModified()));
        });
        return files;
    }

    static List<BucketObject> longListBucket() throws IOException {
        List<BucketObject> files = new ArrayList<>();
        GoogleCredentials scopedCredentials = getWriteCredentials();
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        Page<Blob> page = storage.list(configuration.evaluateToString("gcs.bucket-name"), Storage.BlobListOption.prefix("topic"));
        for (Blob blob : page.iterateAll()) {
            files.add(new BucketObject(blob.getSize(), blob.getName(), blob.isDirectory(), blob.getCreateTime(), blob.getUpdateTime()));
        }
        return files;
    }

    private SpecificationBuilder createMockSpecificationBuilder(int fromPosition,
                                                                int pageSize,
                                                                int stopAtPosition,
                                                                int failAtPosition,
                                                                int failWithStatusCode,
                                                                boolean generateRandomEventItemData,
                                                                int generateEventItemRecordSize,
                                                                int generateEventItemRecordKeySize,
                                                                int generateEventItemRecordElementSize) {
        return Specification.start("TEST-GCS", "Test GCS", "loop")
                .configure(context()
                        .topic("topic")
                        .header("accept", "application/xml")
                        .variable("baseURL", server.testURL(""))
                        .variable("nextPosition", "${contentStream.lastOrInitialPosition(1)}")
                        .variable("pageSize", pageSize)
                        .variable("stopAt", stopAtPosition)
                        .variable("failAt", failAtPosition)
                        .variable("failWithStatusCode", failWithStatusCode)
                        .variable("generateRandomEventItemData", generateRandomEventItemData)
                        .variable("generateEventItemRecordSize", generateEventItemRecordSize)
                        .variable("generateEventItemRecordKeySize", generateEventItemRecordKeySize)
                        .variable("generateEventItemRecordElementSize", generateEventItemRecordElementSize)
                )
                .function(paginate("loop")
                        .variable("fromPosition", "${nextPosition}")
                        .iterate(execute("page"))
                        .prefetchThreshold(5)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .url("${baseURL}/api/events?position=${fromPosition}&pageSize=${pageSize}&stopAt=${stopAt}")
                        .validate(status().success(200, 299))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(nextPage()
                                .output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]position=)[^&]*"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event-id"))
                                )
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPosition")
                )
                .function(get("event-doc")
                        .url("${baseURL}/api/events/${eventId}?generateRandomEventItemData=${generateRandomEventItemData}&generateEventItemRecordSize=${generateEventItemRecordSize}&generateEventItemRecordKeySize=${generateEventItemRecordKeySize}&generateEventItemRecordElementSize=${generateEventItemRecordElementSize}&failAt=${failAt}&failWithStatusCode=${failWithStatusCode}")
                        .validate(status().success(200, 299))
                        .pipe(addContent("${position}", "event-doc"))
                );
    }

//    @Disabled
    @Test
    void thatWorkerCausesUploadFailureToGCS() throws InterruptedException, IOException {
        purgeTempFilesAndBucket();

        final int fromPosition = 1;
        final int pageSize = 100;
        final int stopAtPosition = -1;
        final int failAtPosition = EventItem.EVENT_ID_OFFSET + 1500; // event-id-offset + position-fail-at
        final int failWithStatusCode = HttpStatusCode.HTTP_INTERNAL_ERROR.statusCode();
        final boolean generateRandomEventItemData = true;
        final int generateEventItemRecordSize = 250;
        final int generateEventItemRecordKeySize = 8;
        final int generateEventItemRecordElementSize = 200;

        SpecificationBuilder specificationBuilder = createMockSpecificationBuilder(fromPosition, pageSize, stopAtPosition, failAtPosition, failWithStatusCode,
                generateRandomEventItemData, generateEventItemRecordSize, generateEventItemRecordKeySize, generateEventItemRecordElementSize);

        try {
            CompletableFuture<ExecutionContext> worker = Worker.newBuilder()
                    .configuration(configuration.asMap())
                    .specification(specificationBuilder)
                    .build()
                    .runAsync();

            worker.join();
        } finally {
            long wait = 5;
            System.out.printf("Wait %s seconds to let worker complete.%n", wait);
            System.out.flush();
            TimeUnit.SECONDS.sleep(wait); // keep the test server open during closure

            System.out.printf("List bucket files%n");
            longListBucket().forEach(System.err::println);

            System.out.printf("List remaining local files%n");
            longListTempFiles().forEach(System.err::println);

            System.out.flush();
        }
    }

    @Disabled
    @Test
    void thatServerCausesUploadFailureToGCS() throws IOException, InterruptedException {
        purgeTempFilesAndBucket();

        final int fromPosition = 1;
        final int pageSize = 100;
        final int stopAtPosition = -1;
        final int failAtPosition = EventItem.EVENT_ID_OFFSET + 1500; // event-id-offset + position-fail-at
        final int failWithStatusCode = HttpStatusCode.HTTP_INTERNAL_ERROR.statusCode();
        final boolean generateRandomEventItemData = true;
        final int generateEventItemRecordSize = 250;
        final int generateEventItemRecordKeySize = 8;
        final int generateEventItemRecordElementSize = 200;

        SpecificationBuilder specificationBuilder = createMockSpecificationBuilder(fromPosition, pageSize, stopAtPosition, failAtPosition, failWithStatusCode,
                generateRandomEventItemData, generateEventItemRecordSize, generateEventItemRecordKeySize, generateEventItemRecordElementSize);

        String spec = specificationBuilder.serialize();
        client.put("/tasks", spec);

        try {
            while (true) {
                ResponseHelper<String> taskList = client.get("/tasks");
                String json = taskList.expect200Ok().body();
                ArrayNode taskNodes = JsonParser.createJsonParser().fromJson(json, ArrayNode.class);
                if (taskNodes.size() == 1) {
                    TimeUnit.MILLISECONDS.sleep(500);
                    continue;
                }
                break;
            }
        } finally {
            TimeUnit.SECONDS.sleep(5);

            System.out.printf("List bucket files%n");
            longListBucket().forEach(System.err::println);

            System.out.printf("List remaining local files%n");
            longListTempFiles().forEach(System.err::println);

            System.out.flush();
        }
    }

    static class BucketObject {
        final Long size;
        final String name;
        private final boolean directory;
        private final LocalDateTime createTime;
        private final LocalDateTime updateTime;

        public BucketObject(Long size, String name, boolean directory, Long createTime, Long updateTime) {
            this.size = size;
            this.name = name;
            this.directory = directory;
            this.createTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(createTime), ZoneId.systemDefault());
            this.updateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(updateTime), ZoneId.systemDefault());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketObject that = (BucketObject) o;
            return directory == that.directory &&
                    Objects.equals(size, that.size) &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(createTime, that.createTime) &&
                    Objects.equals(updateTime, that.updateTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(size, name, directory, createTime, updateTime);
        }

        @Override
        public String toString() {
            return "BucketObject{" +
                    "size=" + size +
                    ", name='" + name + '\'' +
                    ", directory=" + directory +
                    ", createTime=" + createTime +
                    ", updateTime=" + updateTime +
                    '}';
        }
    }
}
