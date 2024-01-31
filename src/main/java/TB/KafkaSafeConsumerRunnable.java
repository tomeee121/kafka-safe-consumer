package TB;

import TB.callbacks.OffsetCommitCallback;
import TB.model.Car;
import com.hazelcast.partition.Partition;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaSafeConsumerRunnable implements Runnable {

    private final String topicName;
    private final KafkaConsumer<String, Car> consumer;
    private final OffsetRepository offsetRepository;
    private final EventRepo eventRepo;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaSafeConsumerRunnable(String topicName, KafkaConsumer<String, Car> consumer, OffsetRepository offsetRepository, EventRepo eventRepo) {
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
            } catch (WakeupException | InterruptedException e) {
                // Ignore exception if closing
                if (!closed.get())
                    try {
                        throw e;
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
            } finally {
                try {
                    commitSyncForClosingConsumer(consumer.assignment());
                } catch (CommitFailedException e) {
                    log.error("Synchronous commit offset failed", e);
                } finally {
                    consumer.close();
                }
            }
        }
    }

    //use state in rebalance listener to be up-to-date in case rebalancing happened in the middle of poll() records processing
    private void seekToSpecificOffset() {
        consumer.assignment().forEach(partition -> {
            TopicPartition topicPartition = (TopicPartition) partition;
            Optional<Long> offset = null;
            try {
                offset = offsetRepository.getOffset(topicPartition);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if(offset.isPresent()) {
                long nextOffset = offset.get() + 1;
                log.info("consumer seeking to ");
                consumer.seek(topicPartition, nextOffset);
            }
        });
    }

    private void processRecords() throws InterruptedException {
        if(!closed.get()) {
            ConsumerRecords<String, Car> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Car> consumerRecord : consumerRecords) {
                processSingleEvent(consumerRecord);
            }
        }
    }

    private void processSingleEvent(ConsumerRecord<String, Car> consumerRecord) throws InterruptedException {
        //check for possible duplicate of message by unique attribute
        if(!eventRepo.isEventProcessed(consumerRecord.value().getVin())) {
            log.info("processing partition: {} with value {} offset {}", consumerRecord.partition(), consumerRecord.value(), consumerRecord.offset());
            eventRepo.saveEventId(consumerRecord.value().getVin());
            offsetRepository.storeOffset(consumerRecord);
            consumer.commitAsync(offsetRepository.getPartitionOffsetMap(consumer), new OffsetCommitCallback());
        }
    }

    private void commitSyncForClosingConsumer(Collection<TopicPartition> partitions) {
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

    public void shutdown() {
        this.closed.set(true);
        this.consumer.wakeup();
    }
}
