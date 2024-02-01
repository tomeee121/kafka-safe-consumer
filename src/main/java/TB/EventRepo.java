package TB;

import TB.config.HazelcastConfugration;
import com.hazelcast.collection.IList;

import java.util.UUID;

public class EventRepo {

    public boolean isEventProcessed(UUID eventId) {
        return HazelcastConfugration.getEventIdCache().contains(eventId);
    }

    public void saveEventId(UUID eventId) {
        HazelcastConfugration.addEventIdToListCache(eventId);
    }

    public IList<UUID> getSavedEventIdList() {
        return HazelcastConfugration.getEventIdCache();
    }
}
