package org.kafkaless.util.process

import org.assertj.core.api.Assertions
import org.junit.Test

class EchoMockProcessManagerTest {

    def processManager = new EchoMockProcessManager()

    // Tests

    @Test
    void shouldReturnEcho() {
        def output = processManager.execute(Command.cmd('foo'))
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

    @Test
    void shouldReturnEchoAsynchronously() {
        def output = processManager.executeAsync(Command.cmd('foo')).get()
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

}
