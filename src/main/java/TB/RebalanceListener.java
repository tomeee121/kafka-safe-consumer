package TB;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

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

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("partitions assigned: ");
        partitions.forEach(partition -> log.info("{}, ", partition.partition()));
    }
}
