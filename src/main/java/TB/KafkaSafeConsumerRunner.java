package TB;

import TB.config.KafkaJsonSerializer;
import TB.model.Car;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaSafeConsumerRunner {
    public static void main( String[] args ) throws InterruptedException {
        SpringApplication.run(KafkaSafeConsumerRunner.class, args);
    }

    public void publishRecordsAfterRebalanceOfDeadBroker() throws InterruptedException {
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

        //wait for Hazelcast startup
        Thread.sleep(7000);
        for (int i = 400; i < 800; i++) {
            System.out.println("published within loop: " + i);
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
}
