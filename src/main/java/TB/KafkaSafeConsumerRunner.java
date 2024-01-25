package TB;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaSafeConsumerRunner {
    private static final String TOPIC_NAME = "tb";
    public static void main( String[] args ) {
        KafkaConsumer consumer = new KafkaConsumer(getProperties(), new StringDeserializer(), new JsonDeserializer(Car.class));
        Thread kafka = new Thread(new KafkaSafeConsumerRunnable(TOPIC_NAME, consumer, new OffsetRepository(), new EventRepo()));
        kafka.start();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}
