package org.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class Scheduler {

    @Autowired
    private KStreamer kStreamer;

    @Scheduled(fixedRate = Long.MAX_VALUE) // run once basically. sun will implode before this runs again
    public void schedule() {
        kStreamer.consume();
    }
}
