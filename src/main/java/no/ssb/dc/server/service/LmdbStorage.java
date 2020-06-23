package no.ssb.dc.server.service;

import org.lmdbjava.Env;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

class LmdbStorage implements AutoCloseable {

    final Path databaseDir;
    final Env<ByteBuffer> env;
    final AtomicBoolean closed = new AtomicBoolean(false);

    LmdbStorage(Path databaseDir) {
        this.databaseDir = databaseDir;
        env = createDatabaseEnvironment();
    }

    Env<ByteBuffer> createDatabaseEnvironment() {
        return Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(10_485_760)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(databaseDir.toFile());
    }


    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            env.close();
        }
    }
}
