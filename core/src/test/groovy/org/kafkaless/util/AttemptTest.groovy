package org.kafkaless.util

import org.junit.Test

import static java.util.concurrent.TimeUnit.SECONDS
import static org.assertj.core.api.Assertions.assertThat

class AttemptTest {

    def attempt = new Attempt()

    @Test
    void shouldReturnDefaultResultAfterTimeout() {
        def defaultResult = attempt.tryToReturn(1, SECONDS, 'default') {
            SECONDS.sleep(2)
            'not default'

        }
        assertThat(defaultResult).isEqualTo('default')
    }

    @Test
    void shouldReturnDefaultResult() {
        def defaultResult = attempt.tryToReturn(1, SECONDS, 'default') {
            'not default'
        }
        assertThat(defaultResult).isEqualTo('not default')
    }

}