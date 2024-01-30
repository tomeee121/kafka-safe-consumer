package TB;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.Map;

public class HazelcastConfugration {
    private static Config configBase = new Config();
    private static Ringbuffer<Long> getRingBuffer(String cacheName) {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
        Ringbuffer<Long> ringbuffer = hazelcast.getRingbuffer(cacheName);
        return ringbuffer;
    }
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
    public static Ringbuffer<Long> getSpecificRingBufferCache(String cacheName) {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(configBase);
        Ringbuffer<Long> ringbuffer = hazelcast.getRingbuffer(cacheName);
        return ringbuffer;
    }
}
