package io.qdb.kvstore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes to/from JSON using Jackson.
 */
public class JsonSerializer implements KeyValueStore.Serializer {

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSerializer() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    @Override
    public void serialize(Object value, OutputStream out) throws IOException {
        mapper.writeValue(out, value);
    }

    @Override
    public <T> T deserialize(InputStream in, Class<T> cls) throws IOException {
        return mapper.readValue(in, cls);
    }
}
