package TB;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class OffsetCommitCallback implements org.apache.kafka.clients.consumer.OffsetCommitCallback {
    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if(Objects.nonNull(e)) {
            log.info("error while committing offset during poll {}", e);
        } else {
            log.info("successfully async-commited offset for partition {}");
        }
    }
}
