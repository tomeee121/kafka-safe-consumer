package TB;

import org.apache.kafka.clients.consumer.Consumer;

public class KafkaSafeConsumerRunnable implements Runnable {

    private final String topicName;
    private final Consumer consumer;

    public KafkaSafeConsumerRunnable(String topicName, Consumer consumer) {
        this.topicName = topicName;
        this.consumer = consumer;
    }

    @Override
    public void run() {

    }
}
