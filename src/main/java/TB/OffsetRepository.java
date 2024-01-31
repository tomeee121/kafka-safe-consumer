package TB;

import TB.config.HazelcastConfugration;
import TB.model.Car;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OffsetRepository {

    public Optional<Long> getOffset(TopicPartition partition) throws InterruptedException {
        Ringbuffer<Long> ringbuffer = HazelcastConfugration.getSpecificRingBufferCache(partition.topic() + "_" + partition.partition());
        long sequence = ringbuffer.tailSequence();
        Long item = ringbuffer.readOne(sequence);
        return Optional.ofNullable(item);
    }

    public void storeOffset(ConsumerRecord<String, Car> record) {
        Ringbuffer<Long> ringbuffer = HazelcastConfugration.getSpecificRingBufferCache(record.topic() + "_" + record.partition());
        ringbuffer.add(record.offset());
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap(Consumer consumer) throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
        consumer.assignment().forEach(partition -> {
            String cacheName = ((TopicPartition) partition).topic() + "_" + ((TopicPartition) partition).partition();
            Ringbuffer<Long> ringbuffer = HazelcastConfugration.getSpecificRingBufferCache(cacheName);
            long sequence = ringbuffer.tailSequence();
            Long item = null;
            try {
                item = ringbuffer.readOne(sequence);

                metadataMap.put(new TopicPartition(((TopicPartition) partition).topic(), ((TopicPartition) partition).partition()),
                        new OffsetAndMetadata(item));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        });
        return metadataMap;
    }
}
