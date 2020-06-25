package no.ssb.dc.server.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import no.ssb.dc.api.util.JsonParser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonArrayWriter implements AutoCloseable {
    private final FileOutputStream out;
    private final SequenceWriter sequenceWriter;
    private final int flushAtCount;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ObjectMapper mapper;
    private final JsonParser jsonParser;

    public JsonArrayWriter(Path workPath, String filename, int flushAtCount) {
        this.flushAtCount = flushAtCount;
        try {
            Files.createDirectories(workPath);
            out = new FileOutputStream(workPath.resolve(filename).toFile());
            jsonParser = JsonParser.createJsonParser();
            mapper = jsonParser.mapper();
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            sequenceWriter = writer.writeValues(out);
            sequenceWriter.init(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonParser parser() {
        return jsonParser;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public void write(JsonNode node) {
        try {
            sequenceWriter.write(node);
            if (counter.incrementAndGet() == flushAtCount) {
                sequenceWriter.flush();
                counter.set(0);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            sequenceWriter.flush();
            sequenceWriter.close();
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
