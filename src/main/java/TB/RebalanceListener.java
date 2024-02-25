package TB;

import TB.config.HazelcastConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class RebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private OffsetRepository offsetRepository;
    private final HazelcastConfiguration hazelcastConfiguration;
    private int i = 0;

    public RebalanceListener(OffsetRepository offsetRepository, HazelcastConfiguration hazelcastConfiguration) {
        this.offsetRepository = offsetRepository;
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
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
//        consumer.commitSync(topicPartitionOffsetAndMetadataMap);

        partitions.forEach(partition -> {
            hazelcastConfiguration.clearBuffeerCache(partition.topic() + "_" + partition.partition());
        });
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("partitions assigned: ");
        partitions.forEach(partition -> log.info("{}, ", partition.partition()));

        i++;
        //produce second sort of messages after second partitions assignment-> one broker must be down to trigger it
        if(i == 2) {
            KafkaSafeConsumerRunner kafkaSafeConsumerRunner = new KafkaSafeConsumerRunner();
            try {
                kafkaSafeConsumerRunner.publishRecordsAfterRebalanceOfDeadBroker();
            } catch (InterruptedException e) {
                log.error("Error during producing second sort of messages!");
                throw new RuntimeException(e);
            }
        }

        //start reading from stored offset
        partitions.forEach(partition -> {
            Optional<Long> offset = null;
            try {
                offset = offsetRepository.getOffset(partition);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (offset.isPresent()) {
                //in case first read message is yet to come
                if (offset.get() == 0L) {
                    consumer.seek(partition, 0L);
                } else {
                    long nextOffset = offset.get() + 1;
                    log.info("consumer seeking to ");
//                    consumer.seek(partition, nextOffset);
                }
            }
        });
    }
}
