package TB;

import TB.config.HazelcastConfugration;
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

import java.util.Properties;
import java.util.UUID;

public class KafkaSafeConsumerRunner {
    private static final String TOPIC_NAME = "tb";
    public static void main( String[] args ) throws InterruptedException {
        HazelcastConfugration.addIdCache();

        KafkaConsumer<String, Car> consumer = new KafkaConsumer<String, Car>(getProperties(), new StringDeserializer(), new JsonDeserializer(Car.class));
        Thread kafka = new Thread(new KafkaSafeConsumerRunnable(TOPIC_NAME, consumer, new OffsetRepository(), new EventRepo()));
        kafka.start();

        publishRecords();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    private static void publishRecords() throws InterruptedException {
        final String TOPIC_NAME = "tb";
        final String BOOTSTRAP_SERVERS = "localhost:9091";

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

        // 2. create producer
        KafkaProducer<String, Car> producer = new KafkaProducer<String, Car>(kafkaProps);

        for (int i = 0; i < 200; i++) {
            Thread.sleep(1000);
            ProducerRecord<String, Car> record = new ProducerRecord<>(TOPIC_NAME, "key", new Car("brand: " + i, "model: " + i, UUID.randomUUID()));

            // 4. send data by fire and forget method
            producer.send(record);
        }

        // 5. clear connection
        producer.flush();
        producer.close();
    }
}
