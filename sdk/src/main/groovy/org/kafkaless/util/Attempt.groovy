package org.kafkaless.util

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class Attempt {

    private final def executor = Executors.newCachedThreadPool()

    def <T> T tryToReturn(long timeout, TimeUnit timeUnit, T defaultValue, Task<T> task) {
        def result = executor.submit(new Callable() {
            @Override
            Object call() throws Exception {
                task.call()
            }
        })
        try {
            result.get(timeout, timeUnit)
        } catch (TimeoutException e) {
            result.cancel(true)
            defaultValue
        }
    }

    def close() {
        executor.shutdown()
    }

    static interface Task<T> {

        def <T> T call() throws Exception

    }

}