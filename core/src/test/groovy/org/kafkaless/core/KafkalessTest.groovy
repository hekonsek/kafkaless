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
package org.kafkaless.core

import org.junit.Test
import org.kafkaless.endpoint.management.ManagementService

import static org.assertj.core.api.Assertions.assertThat
import static org.awaitility.Awaitility.await
import static org.kafkaless.util.Uuids.uuid
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent

class KafkalessTest {

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
        management.saveEvent(tenant, 'functions', pipeId, Optional.of(mapEvent(pipe)))
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
        management.saveEvent(tenant, 'functions', pipeId, Optional.of(mapEvent(pipe)))
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
            assertThat(management.countEvents(tenant, 'to')).isGreaterThan(1L)
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

}