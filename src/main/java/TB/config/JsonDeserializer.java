package TB.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer {

    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        T object = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            object = mapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return object;
    }
}
