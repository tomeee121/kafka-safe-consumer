package TB.config;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import lombok.var;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfigBean {

    @Bean
    public ClientConfig clientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("127.0.0.1:5701")
                .setSmartRouting(true)
                .addOutboundPortDefinition("34700-34710")
                .setRedoOperation(true)
                .setConnectionTimeout(5000);
        return clientConfig;
    }
    @Bean
    public HazelcastInstance hazelcastInstance(ClientConfig clientConfig) {
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
}
