package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbEnvironment implements AutoCloseable {

    private final Path databaseDir;
    private final Env<ByteBuffer> env;
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int mapSize;
    private Dbi<ByteBuffer> db;

    public LmdbEnvironment(DynamicConfiguration configuration, Path databaseDir, String topic) {
        this.databaseDir = databaseDir.resolve(topic);
        createDirectories(this.databaseDir);
        this.topic = topic;
        mapSize = configuration != null && configuration.evaluateToString("data.collector.integrityCheck.dbSizeInMb") != null ?
                configuration.evaluateToInt("data.collector.integrityCheck.dbSizeInMb") : 50;
        env = createEnvironment();
    }

    public static void removePath(Path path) {
        try {
            if (path.toFile().exists())
                Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Env<ByteBuffer> env() {
        return env;
    }

    public int maxKeySize() {
        return env.getMaxKeySize();
    }

    public Path getDatabaseDir() {
        return databaseDir;
    }

    private void createDirectories(Path databaseDir) {
        if (!databaseDir.toFile().exists()) {
            try {
                Files.createDirectories(databaseDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Env<ByteBuffer> createEnvironment() {
        return Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(mapSize * 1024 * 1024)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(databaseDir.toFile());
    }

    Dbi<ByteBuffer> open() {
        if (!closed.get() && db != null) {
            return db;
        }
        db = env.openDbi(topic, MDB_CREATE);
        return db;
    }

    void drop() {
        if (!closed.get() && db != null) {
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                db.drop(txn);
            }
        }
    }

    @Override
    public void close() {
        // drop()
        if (closed.compareAndSet(false, true)) {
            env.close();
        }
    }
}
