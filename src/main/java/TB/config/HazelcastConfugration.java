package TB.config;

import TB.listeners.HZIListItemListener;
import TB.listeners.HZRingBufferOffsetListener;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.kafka.common.TopicPartition;

import java.util.UUID;

public class HazelcastConfugration {
    private static Config iListConfig = new Config();
    private static Config ringBufferConfig = new Config();

    private static HazelcastInstance hazelcastIList = null;
    private static HazelcastInstance hazelcastRingBuffer = null;

    public static void clearBuffeerCache(String cacheName) {
        if(hazelcastRingBuffer == null) {
            hazelcastRingBuffer = Hazelcast.newHazelcastInstance(ringBufferConfig);
        }
        Ringbuffer<Long> ringbuffer = hazelcastRingBuffer.getRingbuffer(cacheName);
        ringbuffer.destroy();
    }
    public static void addSpecificRingBufferCache(TopicPartition topicPartition) {
        String cacheName = topicPartition.topic() + "_" + topicPartition.partition();
        if(hazelcastRingBuffer == null) {
            ringBufferConfig.setClusterName("dev");
            ringBufferConfig.getNetworkConfig().setPublicAddress("127.0.0.1").setPort(5703);
            RingbufferConfig ringbufferConfig = ringBufferConfig.getRingbufferConfig(cacheName);
            ringbufferConfig.setCapacity(1)
                    .setBackupCount(1)
                    .setAsyncBackupCount(0)
                    .setTimeToLiveSeconds(1000000)
                    .setInMemoryFormat(InMemoryFormat.BINARY);
            hazelcastRingBuffer = Hazelcast.newHazelcastInstance(ringBufferConfig);
            hazelcastRingBuffer.addDistributedObjectListener(new HZRingBufferOffsetListener());
        }
        hazelcastRingBuffer.getRingbuffer(cacheName);
    }
    public static Ringbuffer<Long> getSpecificRingBufferCache(String cacheName) {
        if(hazelcastRingBuffer == null) {
            hazelcastRingBuffer = Hazelcast.newHazelcastInstance(ringBufferConfig);
        }
        Ringbuffer<Long> ringbuffer = hazelcastRingBuffer.getRingbuffer(cacheName);
        return ringbuffer;
    }
    public static void addIListForEventIdCache() {
        iListConfig.setClusterName("dev");
        iListConfig.getNetworkConfig().setPublicAddress("127.0.0.1").setPort(5703);
        ListConfig listConfig = iListConfig.getListConfig("id");
        listConfig.setMaxSize(Integer.MAX_VALUE)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        hazelcastIList = Hazelcast.newHazelcastInstance(iListConfig);

    }
    public static IList<UUID> getEventIdCache() {
        if(hazelcastIList == null) {
            hazelcastIList = Hazelcast.newHazelcastInstance(iListConfig);
            IList<UUID> hazelcastIdList = hazelcastIList.getList("id");
            hazelcastIdList.addItemListener( new HZIListItemListener(), true );
        }
        return hazelcastIList.getList("id");
    }

    public static void addEventIdToListCache(UUID uuid) {
        if(hazelcastIList == null) {
            hazelcastIList = Hazelcast.newHazelcastInstance(iListConfig);
        }
        hazelcastIList.getList("id").add(uuid);
    }
}
