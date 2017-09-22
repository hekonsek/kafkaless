/**
 * Licensed to the Kafkaless under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kafkaless.org.kafkaless.sdk

import org.junit.Test
import org.kafkaless.sdk.Kafkaless
import org.kafkaless.sdk.Pipe
import org.kafkaless.sdk.api.Event
import org.kafkaless.sdk.api.KafkalessBuilder
import org.kafkaless.endpoint.management.ManagementService

import java.util.concurrent.Callable
import java.util.concurrent.Executors

import static java.lang.Math.abs
import static java.lang.System.currentTimeMillis
import static org.assertj.core.api.Assertions.assertThat
import static org.awaitility.Awaitility.await
import static org.kafkaless.sdk.Kafkaless.PIPES_TOPIC
import static org.kafkaless.util.Uuids.uuid
import static org.kafkaless.util.kafka.DockerizedKafka.ensureKafkaIsRunning
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent

class KafkalessTest {

    // Kafka broker fixtures

    static {
        ensureKafkaIsRunning()
    }

    // Kafkaless fixtures

    def tenant = uuid()

    def kafkaless = new KafkalessBuilder("--TENANT=${tenant}").build()

    def template = kafkaless.@kafkaTemplate

    // Management service fixtures

    def management = new ManagementService(template)

    // Pipes fixtures

    def functionName = uuid()

    def pipeId = uuid()

    def to = uuid()

    def from = uuid()

    def pipe = new Pipe(from: from, function: functionName, to: to)

    def event = [metadata: [metaNumber: 666], payload: [foo: 'bar']]

    // Tests

    @Test
    void shouldExecuteFunction() {
        // Given
        management.saveEvent(tenant, PIPES_TOPIC, pipeId, Optional.of(mapEvent(pipe)))
        kafkaless.functionHandler(functionName) {
            it
        }

        // When
        management.saveEvent(tenant, from, 'key', Optional.of(mapEvent(event)))

        // Then
        await().untilAsserted {
            assertThat(management.countEvents(tenant, to)).isEqualTo(1L)
        }
    }

    @Test
    void shouldRedirectEventToErrorQueue() {
        // Given
        management.saveEvent(tenant, PIPES_TOPIC, pipeId, Optional.of(mapEvent(pipe)))
        kafkaless.functionHandler(functionName) {
            throw new RuntimeException()
        }

        // When
        management.saveEvent(tenant, from, 'key', Optional.of(mapEvent(event)))

        // Then
        await().untilAsserted {
            assertThat(management.countErrorEvents(tenant, pipeId)).isEqualTo(1L)
        }
    }

    @Test
    void shouldLoadFunctionFromJson() {
        // Given
        def tenant = 'jsonTenant'
        def kafkaless = new Kafkaless(template, tenant)
        kafkaless.functionHandler('functionFromJson') {
            it
        }

        // When
        management.saveEvent(tenant, 'from', 'key', Optional.of(mapEvent(event)))

        // Then
        await().untilAsserted {
            assertThat(management.countEvents(tenant, 'to')).isGreaterThan(0L)
        }
    }

    @Test
    void shouldInvokeFunction() {
        // Given
        kafkaless.functionHandler(functionName) {
            it
        }

        // When
        def response = management.invoke(tenant, functionName, event)

        // Then
        assertThat(response).isNotEmpty()
    }

    @Test
    void shouldInvokeFunctionConcurrently() {
        // Given
        def executor = Executors.newCachedThreadPool()
        List<Long> executions = []

        kafkaless.functionHandler(functionName) {
            executions << currentTimeMillis()
            it
        }

        // When
        def first = executor.submit(new Callable() {
            @Override
            Object call() throws Exception {
                management.invoke(tenant, functionName, event)
            }
        })
        def second = executor.submit(new Callable() {
            @Override
            Object call() throws Exception {
                management.invoke(tenant, functionName, event)
            }
        })
        first.get()
        second.get()

        // Then
        assertThat(abs(executions[0] - executions[1])).isLessThan(1000L)
    }

    @Test
    void shouldInvokeSubsequentFunctions() {
        // Given
        kafkaless.functionHandler(functionName) {
            it
        }

        // When
        management.invoke(tenant, functionName, event)
        management.invoke(tenant, functionName, event)
        def response = management.invoke(tenant, functionName, [metadata: [:], payload: [order: 'lastOne']])

        // Then
        assertThat(response.payload as Map).containsEntry('order', 'lastOne')
    }

    @Test
    void shouldInvokeFunctionFromFunction() {
        // Given
        kafkaless.functionHandler(functionName) {
            def inv = kafkaless.invoke('helloFunction', it.metadata(), it.payload().get())
            new Event(it.key(), inv.metadata as Map, Optional.of(inv.payload as Map))
        }
        kafkaless.functionHandler('helloFunction') {
            it.payload().get().hello = 'world'
            it
        }

        // When
        def response = management.invoke(tenant, functionName, event)

        // Then
        assertThat(response.payload.hello).isEqualTo('world')
    }

}