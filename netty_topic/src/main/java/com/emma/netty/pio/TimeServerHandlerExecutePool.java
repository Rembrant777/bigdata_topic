package com.emma.netty.pio;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimeServerHandlerExecutePool {
    private ExecutorService executor;

    public TimeServerHandlerExecutePool(int maxPoolSize, int queueSize) {
        this.executor = new ThreadPoolExecutor(
                // processor num
                Runtime.getRuntime().availableProcessors(),

                // max pools zie
                maxPoolSize,

                // when the number of threads is greater than the core,
                // this is the maximum time that excess idle threads will wait for new tasks before terminating.
                120L,

                // time unit
                TimeUnit.SECONDS,

                // The queue to use for holding tasks before they are executed.
                // This queue will hold only the Runnable tasks submitted by the execute method.
                new ArrayBlockingQueue<Runnable>(queueSize));
    }

    public void execute(Runnable task) {
        executor.execute(task);
    }
}
