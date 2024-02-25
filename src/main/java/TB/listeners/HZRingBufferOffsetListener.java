package TB.listeners;

import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;

public class HZRingBufferOffsetListener implements DistributedObjectListener {

    @Override
    public void distributedObjectCreated(DistributedObjectEvent distributedObjectEvent) {
//        System.out.println( "Offset added to store:  " + distributedObjectEvent.getDistributedObject() );
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent distributedObjectEvent) {
//        System.out.println( "Offset removed: " + distributedObjectEvent.getDistributedObject() );
    }
}