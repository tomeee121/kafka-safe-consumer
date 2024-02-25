package TB.listeners;

import TB.EventRepo;
import TB.KafkaSafeConsumerRunnable;
import TB.OffsetRepository;
import TB.config.HazelcastConfiguration;
import TB.config.JsonDeserializer;
import TB.config.KafkaJsonSerializer;
import TB.model.Car;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class Startup {
    private final HazelcastConfiguration hazelcastConfiguration;

    public Startup(HazelcastConfiguration hazelcastConfiguration) {
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runner() throws InterruptedException {
        hazelcastConfiguration.addIListForEventIdCache();

        EventRepo eventRepo = new EventRepo(hazelcastConfiguration);
        KafkaConsumer<String, Car> consumer = new KafkaConsumer<String, Car>(getProperties(), new StringDeserializer(), new JsonDeserializer(Car.class));
        KafkaSafeConsumerRunnable kafkaSafeConsumerRunnable = new KafkaSafeConsumerRunnable(new OffsetRepository(hazelcastConfiguration), eventRepo, hazelcastConfiguration);
        kafkaSafeConsumerRunnable.setConsumer(consumer);
        Thread kafka = new Thread(kafkaSafeConsumerRunnable);
        kafka.start();

        Thread readEventProcessedRunnable = new Thread(new ReadEventProcessedRunnable(eventRepo));
        readEventProcessedRunnable.start();

        publishRecords();
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "8000");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "90000");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "3000");
        return properties;
    }

    private void publishRecords() throws InterruptedException {
        final String TOPIC_NAME = "tb";
        final String BOOTSTRAP_SERVERS = "localhost:9091";

        Properties kafkaProps = getProperties(BOOTSTRAP_SERVERS);

        // 2. create producer
        KafkaProducer<String, Car> producer = new KafkaProducer<String, Car>(kafkaProps);

        //wait for Hazelcast startup
        Thread.sleep(7000);
        for (int i = 0; i < 400; i++) {
            Thread.sleep(100);
//            System.out.println("published within loop: " + i);
            TB.model.UUID uuidAbleToRecap = new TB.model.UUID(Long.valueOf(String.format("%03d", i)));
            ProducerRecord<String, Car> record =
                    new ProducerRecord<>(TOPIC_NAME, "key", new Car("brand: " + i, "model: " + i, uuidAbleToRecap));

            // 4. send data by fire and forget method
            producer.send(record);
        }

        // 5. clear connection
        producer.flush();
        producer.close();
    }

    private static Properties getProperties(String BOOTSTRAP_SERVERS) {
        // 1. create producer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        // performence settings
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,3);

        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return kafkaProps;
    }
}
