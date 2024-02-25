package TB.listeners;

import TB.EventRepo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadEventProcessedRunnable implements Runnable {
    private final EventRepo eventRepo;

    public ReadEventProcessedRunnable(EventRepo eventRepo) {
        this.eventRepo = eventRepo;
    }

    @Override
    public void run() {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
//            log.info("INFO. Processed event ids of size {}:", eventRepo.getSavedEventIdList().size());
            for (int i1 = 0; i1 < eventRepo.getSavedEventIdList().size(); i1++) {
                if(i1 % 10 == 0) {
//                    System.out.println(eventRepo.getSavedEventIdList().get(i1));
                }
            }
//            eventRepo.getSavedEventIdList().listIterator().forEachRemaining(System.out::println);
        }
    }
}
