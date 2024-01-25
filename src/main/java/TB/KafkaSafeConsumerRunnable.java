package TB;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaSafeConsumerRunnable implements Runnable {

    private final String topicName;
    private final KafkaConsumer consumer;
    private final OffsetRepository offsetRepository;
    private final EventRepo eventRepo;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaSafeConsumerRunnable(String topicName, KafkaConsumer consumer, OffsetRepository offsetRepository, EventRepo eventRepo) {
        this.topicName = topicName;
        this.consumer = consumer;
        this.offsetRepository = offsetRepository;
        this.eventRepo = eventRepo;
    }

    @Override
    public void run() {
        RebalanceListener rebalanceListener = new RebalanceListener(consumer, offsetRepository);
        while (!closed.get()) {
            try {
                consumer.subscribe(Collections.singleton(topicName), rebalanceListener);
                seekToSpecificOffset();
                processRecords();
            } finally {
                consumer.commitSync(offsetRepository.getPartitionOffsetMap());
                consumer.close();
            }
        }
    }

    //use state in rebalance listener to be up-to-date in case rebalancing happened in the middle of poll() records processing
    private void seekToSpecificOffset() {
        consumer.assignment().forEach(partition -> {
            TopicPartition topicPartition = (TopicPartition) partition;
            Optional<Long> offset = offsetRepository.getOffset(topicPartition);
            if(offset.isPresent()) {
                long nextOffset = offset.get() + 1;
                log.info("consumer seeking to ");
                consumer.seek(topicPartition, nextOffset);
            }
        });
    }

    private void processRecords() {
        if(!closed.get()) {
            ConsumerRecords<String, Car> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Car> consumerRecord : consumerRecords) {
                processSingleEvent(consumerRecord);
            }
        }
    }

    private void processSingleEvent(ConsumerRecord<String, Car> consumerRecord) {
        //check for possible duplicate of message by unique attribute
        if(!eventRepo.isEventProcessed(consumerRecord.value().getVin())) {
            log.info("processing partition: {} with value {} offset {}", consumerRecord.partition(), consumerRecord.value(), consumerRecord.offset());
            eventRepo.saveEventId(consumerRecord.value().getVin());
            offsetRepository.storeOffset(consumerRecord);
            consumer.commitAsync(offsetRepository.getPartitionOffsetMap(), new OffsetCommitCallback());
        }
    }

    public void shutdown() {
        this.closed.set(true);
        this.consumer.wakeup();
    }
}
