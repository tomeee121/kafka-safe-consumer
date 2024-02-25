package TB.listeners;

import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;

import java.util.UUID;

public class HZIListItemListener implements ItemListener<Long> {

    @Override
    public void itemAdded(ItemEvent<Long> event) {
        System.out.println( "Item added:  " + event );
    }

    @Override
    public void itemRemoved(ItemEvent<Long> event) {
        System.out.println( "Item removed: " + event );
    }
}