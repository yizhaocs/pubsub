package com.adara.hackathon.sub;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SubMain {
    private BQWriter bqWriter;

    public void init() throws Exception{
        bqWriter = new BQWriter();
        ScheduledExecutorService thread = Executors.newSingleThreadScheduledExecutor();
        SubTask mSubTask = new SubTask(bqWriter);
        int refreshInterval = 10; // seconds
        thread.scheduleWithFixedDelay(mSubTask, refreshInterval, refreshInterval, TimeUnit.SECONDS);
    }

    public void destroy() throws Exception{
    }
}
