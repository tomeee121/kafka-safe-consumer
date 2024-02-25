package TB;

import TB.config.HazelcastConfiguration;
import com.hazelcast.collection.IList;
import org.springframework.stereotype.Component;

@Component
public class EventRepo {

    private final HazelcastConfiguration hazelcastConfiguration;

    public EventRepo(HazelcastConfiguration hazelcastConfiguration) {
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public boolean isEventProcessed(TB.model.UUID eventId) {
//        return hazelcastConfiguration.getEventIdCache().contains(eventId.getUuid());
        return false;
    }

    public void saveEventId(TB.model.UUID eventId) {
//        hazelcastConfiguration.addEventIdToListCache(eventId);
    }

    public IList<Long> getSavedEventIdList() {
        return hazelcastConfiguration.getEventIdCache();
    }
}
