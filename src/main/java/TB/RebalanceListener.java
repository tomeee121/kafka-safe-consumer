package TB;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private OffsetRepository offsetRepository;

    public RebalanceListener(KafkaConsumer consumer, OffsetRepository offsetRepository) {
        this.consumer = consumer;
        this.offsetRepository = offsetRepository;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("partitions revoked: ");
        partitions.forEach(partition -> log.info("{}, ", partition.partition()));

        //commit sync
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = new HashMap<>();

        partitions.forEach(partition -> {
            TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
            Long offset = null;
            try {
                offset = offsetRepository.getOffset(topicPartition).orElseThrow(() ->
                        new IllegalStateException(String.format("No cached offset for given TopicPartition %s", topicPartition)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            topicPartitionOffsetAndMetadataMap.put(topicPartition, new OffsetAndMetadata(offset));
        });
        consumer.commitSync(topicPartitionOffsetAndMetadataMap);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("partitions assigned: ");
        partitions.forEach(partition -> log.info("{}, ", partition.partition()));

        partitions.forEach(partition -> {
            HazelcastConfugration.addBufferCache(partition.topic() + "_" + partition.partition());
        });
    }
}
