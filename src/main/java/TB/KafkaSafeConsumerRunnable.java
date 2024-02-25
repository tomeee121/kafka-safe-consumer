package TB;

import TB.callbacks.OffsetCommitCallback;
import TB.config.HazelcastConfiguration;
import TB.model.Car;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class KafkaSafeConsumerRunnable implements Runnable {
    private static final String topicName = "tb";
    private KafkaConsumer<String, Car> consumer;
    private final OffsetRepository offsetRepository;
    private final EventRepo eventRepo;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final HazelcastConfiguration hazelcastConfiguration;

    public KafkaSafeConsumerRunnable(OffsetRepository offsetRepository, EventRepo eventRepo, HazelcastConfiguration hazelcastConfiguration) {
        this.offsetRepository = offsetRepository;
        this.eventRepo = eventRepo;
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public void setConsumer(KafkaConsumer<String, Car> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        RebalanceListener rebalanceListener = new RebalanceListener(offsetRepository, hazelcastConfiguration);
        rebalanceListener.setConsumer(consumer);
        while (!closed.get()) {
            try {
                consumer.subscribe(Collections.singleton(topicName), rebalanceListener);
//                redundand for now (rebalance listener implemented)
//                seekToSpecificOffset();
                processRecords();
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) {
                    try {
                        throw e;
                    } catch (WakeupException ex) {
                        consumer.close();
                        throw new RuntimeException(ex);
                    }
                }
            } catch (InterruptedException e) {
                if (!closed.get()) {
                    try {
                    throw e;
                } catch (InterruptedException ex) {
                        consumer.close();
                        throw new RuntimeException(ex);
                }
            }
            }
            finally {
                try {
//                    commitSyncForClosingConsumer(consumer.assignment());
                } catch (CommitFailedException e) {
                    log.error("Synchronous commit offset failed", e);
                    consumer.close();
                }
            }
        }
    }

    //alternative to rebalance listener impl for taking care of offset
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

                //in case first read message is yet to come
                if(offset.get() == 0L) {
                    consumer.seek(topicPartition, 0L);
                    return;
                }

                long nextOffset = offset.get() + 1;
                log.info("consumer seeking to ");
                consumer.seek(topicPartition, nextOffset);
            }
        });
    }

    private void processRecords() throws InterruptedException {
        if(!closed.get()) {
            ConsumerRecords<String, Car> consumerRecords = consumer.poll(Duration.ofMillis(10000));
            consumerRecords.partitions().iterator().forEachRemaining(topicPartition -> {
                hazelcastConfiguration.addSpecificRingBufferCache(topicPartition);

                consumerRecords.records(topicPartition).forEach(record -> {
                    try {
                        processSingleEvent(record);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            });
        }
    }

    private void processSingleEvent(ConsumerRecord<String, Car> consumerRecord) throws InterruptedException {
        //check for possible duplicate of message by unique attribute
        if(!eventRepo.isEventProcessed(consumerRecord.value().getVin())) {
            log.info("Processed vin: " + consumerRecord.value().getVin());
            log.debug("processing partition: {} with value {} offset {}", consumerRecord.partition(), consumerRecord.value(), consumerRecord.offset());
            eventRepo.saveEventId(consumerRecord.value().getVin());
            offsetRepository.storeOffset(consumerRecord);
//            consumer.commitAsync(offsetRepository.getPartitionOffsetMap(consumer), new OffsetCommitCallback());
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
//            consumer.commitSync(topicPartitionOffsetAndMetadataMap);
    }

    public void shutdown() {
        this.closed.set(true);
        this.consumer.wakeup();
    }
}
