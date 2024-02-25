package TB.config;

import TB.listeners.HZIListItemListener;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class HazelcastConfiguration {
    private IList<Long> hazelcastIList;
    private Map<String, Ringbuffer<Long>> hazelcastRingBuffer = new HashMap<>();

    private final HazelcastInstance hazelcastInstance;

    public HazelcastConfiguration(HazelcastInstance instance) {
        this.hazelcastInstance = instance;
    }

    public void clearBuffeerCache(String cacheName) {
        if(hazelcastRingBuffer != null) {
            hazelcastInstance.getRingbuffer(cacheName).destroy();
        }
    }
    public void addSpecificRingBufferCache(TopicPartition topicPartition) {
        String cacheName = topicPartition.topic() + "_" + topicPartition.partition();
        if(hazelcastRingBuffer.size() < 3) {
            hazelcastRingBuffer.put(cacheName, hazelcastInstance.getRingbuffer(cacheName));
        }
    }
    public Ringbuffer<Long> getSpecificRingBufferCache(String cacheName) {
        if(hazelcastRingBuffer == null) {
            hazelcastRingBuffer = Map.of(cacheName, hazelcastInstance.getRingbuffer(cacheName));
        }
        if(!hazelcastRingBuffer.containsKey(cacheName)) {
            hazelcastRingBuffer.put(cacheName, hazelcastInstance.getRingbuffer(cacheName));
        }
        for (Map.Entry<String, Ringbuffer<Long>> buff : hazelcastRingBuffer.entrySet()) {
            if(buff.getKey().equals(cacheName)) {
                return buff.getValue();
            }
        }
        return hazelcastRingBuffer.get(cacheName);
    }
    public void addIListForEventIdCache() {
        if(hazelcastIList == null) {
            hazelcastIList = hazelcastInstance.getList("id");
            ItemListener<Long> itemListener = new HZIListItemListener();
            hazelcastIList.addItemListener(itemListener, true);
        }
    }
    public IList<Long> getEventIdCache() {
        if(hazelcastIList == null) {
            hazelcastIList = hazelcastInstance.getList("id");
        }
        return hazelcastIList;
    }

    public void addEventIdToListCache(TB.model.UUID uuid) {
        if(hazelcastIList == null) {
            hazelcastIList = hazelcastInstance.getList("id");
        }
        hazelcastIList.add(uuid.getUuid());
    }
}
