package TB;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;

public class OffsetRepository {
    Buffer buf = new CircularFifoBuffer(4);

    public Optional<Long> getOffset(TopicPartition partition) {
        return Optional.ofNullable(Long.valueOf(String.valueOf(buf.get())));
    }

    public void storeOffset(ConsumerRecord<String, Car> record) {

    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return Map.of();
    }
}
