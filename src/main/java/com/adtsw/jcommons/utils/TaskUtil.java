package com.adtsw.jcommons.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TaskUtil {

    private static final Logger logger = LogManager.getLogger(TaskUtil.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(12);
    private static final Map<String, ScheduledFuture<?>> taskHandles = new HashMap<>();

    public static void scheduleTask(String taskName, long initialDelay, long period, 
                                    int variationInSeconds, int timeoutInSeconds, 
                                    TimeUnit unit, Task task) {
    
        if(taskHandles.containsKey(taskName)) {
            throw new RuntimeException("task " + taskName + " already exists");
        }
        
        logger.info("scheduling " + taskName + " at period " + period);
        final ScheduledFuture<?> taskHandle = scheduler.scheduleAtFixedRate(
            new ScheduledTask(taskName, task, variationInSeconds, timeoutInSeconds), initialDelay, period, unit
        );

        taskHandles.put(taskName, taskHandle);
    }
    
    public static boolean cancelScheduledTask(String taskName) {

        if(!taskHandles.containsKey(taskName)) {
            return false;
        }
        
        logger.info("cancelling task " + taskName);
        taskHandles.get(taskName).cancel(true);
        taskHandles.remove(taskName);
        return true;
    }
    
    public static void shutdown() {
        
        taskHandles.forEach((taskName, taskHandle) -> {
            logger.info("cancelling task " + taskName);
            taskHandle.cancel(true);
        });
        scheduler.shutdown();
    }
}