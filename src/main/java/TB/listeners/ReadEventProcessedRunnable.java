package TB.listeners;

import TB.EventRepo;

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
            System.out.println(String.format("INFO. Processed event ids:"));
            eventRepo.getSavedEventIdList().listIterator().forEachRemaining(System.out::println);
        }
    }
}
