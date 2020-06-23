package no.ssb.dc.server.service;

import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LmdbStorageTest {

    @Test
    public void dbEnv() throws IOException {
        Path dbPath = CommonUtils.currentPath().resolve("target").resolve("lmdb");
        if (!dbPath.toFile().exists()) {
            Files.createDirectories(dbPath);
        }
        try (LmdbStorage db = new LmdbStorage(dbPath)) {
            
        }
    }
}
