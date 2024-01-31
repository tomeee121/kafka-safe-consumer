package TB;

import TB.config.HazelcastConfugration;

import java.util.UUID;

public class EventRepo {

    public boolean isEventProcessed(UUID eventId) {
        return HazelcastConfugration.getEventIdCache().contains(eventId);
    }

    public void saveEventId(UUID eventId) {
        HazelcastConfugration.getEventIdCache().add(eventId);
    }
}
