package TB.config;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.Map;
import java.util.UUID;

public class HazelcastConfugration {
    private static Config configBase = new Config();
    public static void addBufferCache(String cacheName) {
        configBase.setClusterName("dev");
        configBase.getNetworkConfig().setPublicAddress("127.0.0.1").setPort(5703);
        RingbufferConfig ringbufferConfig = configBase.getRingbufferConfig(cacheName);
        ringbufferConfig.setCapacity(1)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(1000000)
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    public static void clearBuffeerCache(String cacheName) {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
        Ringbuffer<Long> ringbuffer = hazelcast.getRingbuffer(cacheName);
        ringbuffer.destroy();
    }
    public static Ringbuffer<Long> getSpecificRingBufferCache(String cacheName) {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
        Ringbuffer<Long> ringbuffer = hazelcast.getRingbuffer(cacheName);
        return ringbuffer;
    }
    public static void addIdCache() {
        configBase.setClusterName("dev");
        configBase.getNetworkConfig().setPublicAddress("127.0.0.1").setPort(5703);
        ListConfig listConfig = configBase.getListConfig("id");
        listConfig.setMaxSize(Integer.MAX_VALUE)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
    }
    public static IList<UUID> getEventIdCache() {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
        IList<UUID> hazelcastIdList = hazelcast.getList("id");
        return hazelcastIdList;
    }

//    private static Ringbuffer<Long> getRingBuffer(String cacheName) {
//        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
//        Ringbuffer<Long> ringbuffer = hazelcast.getRingbuffer(cacheName);
//        return ringbuffer;
//    }
}
