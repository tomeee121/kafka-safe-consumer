package TB;

import TB.config.HazelcastConfiguration;
import TB.model.Car;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class OffsetRepository {

    private final HazelcastConfiguration hazelcastConfiguration;

    public OffsetRepository(HazelcastConfiguration hazelcastConfiguration) {
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public Optional<Long> getOffset(TopicPartition partition) throws InterruptedException {
        Ringbuffer<Long> ringbuffer = hazelcastConfiguration.getSpecificRingBufferCache(partition.topic() + "_" + partition.partition());
        long sequence = ringbuffer.tailSequence();
        if(sequence < 0) {
            return Optional.ofNullable(0L);
        }
        Long item = ringbuffer.readOne(sequence);
        return Optional.ofNullable(item);
    }

    public void storeOffset(ConsumerRecord<String, Car> record) {
        Ringbuffer<Long> ringbuffer = hazelcastConfiguration.getSpecificRingBufferCache(record.topic() + "_" + record.partition());
        ringbuffer.add(record.offset());
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap(Consumer consumer) throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
        consumer.assignment().forEach(partition -> {
            String cacheName = ((TopicPartition) partition).topic() + "_" + ((TopicPartition) partition).partition();
            Ringbuffer<Long> ringbuffer = hazelcastConfiguration.getSpecificRingBufferCache(cacheName);
            long sequence = ringbuffer.tailSequence();
            Long item = null;
            try {
                if(sequence < 0) {
                    item = 0L;
                } else {
                    item = ringbuffer.readOne(sequence);
                }

                metadataMap.put(new TopicPartition(((TopicPartition) partition).topic(), ((TopicPartition) partition).partition()),
                        new OffsetAndMetadata(item));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        });
        return metadataMap;
    }
}
