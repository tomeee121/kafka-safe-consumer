package TB.listeners;

import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;

import java.util.UUID;

public class HZIListItemListener implements ItemListener<UUID> {

    @Override
    public void itemAdded(ItemEvent<UUID> event) {
        System.out.println( "Item added:  " + event );
    }

    @Override
    public void itemRemoved(ItemEvent<UUID> event) {
        System.out.println( "Item removed: " + event );
    }
}