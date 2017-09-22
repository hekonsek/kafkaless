package org.kafkaless.util.process

import com.google.common.collect.ImmutableList

class FixedMockProcessManager extends ExecutorBasedProcessManager {

    private final List<String> result

    FixedMockProcessManager(String... result) {
        this.result = ImmutableList.copyOf(result)
    }

    @Override
    List<String> execute(Command command) {
        result
    }

}