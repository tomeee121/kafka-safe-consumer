package TB.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class KafkaJsonSerializer<T> implements Serializer {

    @Override
    public byte[] serialize(String s, Object o) {
        ObjectMapper mapper = new ObjectMapper();
        byte[] result = null;
        try {
            result = mapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            log.error("error during serialization");
        }
        return result;
    }
}
