/**
 * Licensed to the KPipes under one or more
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
package kpipes.binding

import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Ignore
import org.junit.runner.RunWith
import org.kafkaless.sdk.Pipe

import static org.kafkaless.util.Json.jsonBytes
import static org.kafkaless.util.Uuids.uuid

@Ignore
@RunWith(VertxUnitRunner)
class KafkaBindingTest {
    
    // Fixtures

    def tenant = uuid()

    def user = uuid()

    def namespace = uuid()

    def topic = uuid()

    def streamName = uuid()

    def streamTopic = "data.${tenant}.${namespace}.${streamName}"

    def key = uuid()

    def event = [foo: uuid()]

    def eventBytes = jsonBytes(event)

    // Service operations fixtures
    
    def clientId = uuid()

    def requestId = uuid()

    def response = uuid()

    def operation = uuid()

    // Pipe fixtures

    def pipeId = uuid()

    def fromNamespace = uuid()

    def from = uuid()

    def function = uuid()

    def toNamespace = uuid()

    def to = uuid()

    def pipe = new Pipe(tenant: tenant, fromNamespace: fromNamespace, from: from, function: function, toNamespace: namespace, to: to)

//    @Test(timeout = 10000L)
//    void shouldTailDataErrorEvents(TestContext context) {
//        def async = context.async()
//
//        binding.sendEvent(tenant, user, namespace, streamName, uuid(), Optional.of(eventBytes))
//        binding.sendEvent(tenant, user, namespace, streamName, uuid(), Optional.of(eventBytes))
//        binding.sendEvent(tenant, user, namespace, streamName, uuid(), Optional.of(eventBytes))
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.${streamName}")).errorTopic("error.${pipeId}")) {
//            throw new RuntimeException('Ops!')
//        }
//
//        binding.subscribe(new ConsumerConfig(topics("error.${pipeId}"))) {
//            def tail = binding.tailDataEventsErrors(pipeId, 2)
//            assertThat(tail).hasSize(2)
//            async.complete()
//        }
//    }
//
//    @Test
//    void optionalOperationShouldUnboxValue(TestContext context) {
//        // Given
//        def async = context.async()
//        binding.registerServiceOperation('service', operation) {
//            Optional.of('Hello!')
//        }
//
//        binding.subscribeForResponseEvents(uuid(), clientId) {
//            // Then
//            def response = fromJson(it.value())
//            assertThat(response.response).isEqualTo('Hello!')
//            async.complete()
//        }
//
//        // When
//        binding.sendRequestEvent(tenant, user, clientId, requestId, 'service', operation, payload)
//    }
//
//    @Test
//    void emptyOptionalOperationShouldReturnNull(TestContext context) {
//        // Given
//        def async = context.async()
//        binding.registerServiceOperation('service', operation) {
//            Optional.empty()
//        }
//
//        binding.subscribeForResponseEvents(uuid(), clientId) {
//            // Then
//            def response = fromJson(it.value())
//            assertThat(response.response).isNull()
//            async.complete()
//        }
//
//        // When
//        binding.sendRequestEvent(tenant, user, clientId, requestId, 'service', operation, payload)
//    }
//
//    @Test
//    void shouldReturnErrorMessage(TestContext context) {
//        // Given
//        def async = context.async()
//        binding.registerServiceOperation('service', 'errorOperation') {
//            throw new RuntimeException('Ops!')
//        }
//
//        binding.subscribeForResponseEvents(uuid(), clientId) {
//            // Then
//            def response = fromJson(it.value())
//            def errorMessage = notBlankString(response, 'error')
//            assertThat(errorMessage).isEqualTo('Ops!')
//            async.complete()
//        }
//
//        // When
//        binding.sendRequestEvent(tenant, user, clientId, requestId, 'service', 'errorOperation', payload)
//    }
//
//    @Test
//    void shouldReceiveMetadataWithTenantAndUser(TestContext context) {
//        // Given
//        def async = context.async()
//
//        binding.subscribeForRequestEvents(uuid(), 'service', operation) {
//            // Then
//            def metadata = notNullPojo(fromJson(it.value()), 'metadata', Map)
//            assertThat(metadata).containsKeys('tenant', 'user')
//            async.complete()
//        }
//        Thread.sleep(1000)
//
//        // When
//        binding.sendRequestEvent(tenant, user, clientId, requestId, 'service', operation, payload)
//    }
//
//    // Pipes tests
//
//    @Test
//    void shouldAddPipe() {
//        // Given
//        binding.materializePipes(new InMemoryKeyValueMaterializedView<Pipe>())
//
//        // When
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: 'myfunction', toNamespace: namespace, to: 'to'))
//
//        // Then
//        await().until({binding.listPipes().containsKey(pipeId)} as Callable<Boolean>)
//    }
//
//    @Test(expected = IllegalArgumentException)
//    void addOperationShouldValidateBlankPipeId() {
//        def blankPipeId = ''
//        binding.addPipe(blankPipeId, new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: function, toNamespace: namespace, to: 'to'))
//    }
//
//    @Test
//    void shouldDeletePipe() {
//        // Given
//        binding.materializePipes(new InMemoryKeyValueMaterializedView<Pipe>())
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: function, toNamespace: namespace, to: 'to'))
//        await().until({binding.listPipes().containsKey(pipeId)} as Callable<Boolean>)
//
//        // When
//        binding.deletePipe(pipeId)
//
//        // Then
//        await().until({!binding.listPipes().containsKey(pipeId)} as Callable<Boolean>)
//    }
//
//    @Test
//    void shouldRegisterFunction(TestContext context) {
//        def async = context.async()
//
//        binding.functionHandler('myfunction2') {
//            it.payload()
//        }
//        binding.addPipe(uuid(), new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: 'myfunction2', toNamespace: namespace, to: 'to'))
//
//        binding.sendEvent(tenant, user, namespace, 'from', key, [:])
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.to"))) {
//            async.complete()
//        }
//    }
//
//    @Test
//    void functionWithManualOutputShouldNotSendMessageToTargetTopic(TestContext context) {
//        // Given
//        def async = context.async()
//        binding.functionHandler(pipe.function, true) {
//            async.complete()
//        }
//        binding.addPipe(pipeId, pipe)
//
//        // When
//        binding.sendEvent(tenant, user, fromNamespace, from, key, payload)
//
//        // Then
//        assertThat(binding.countDataEvents(tenant, null, namespace, to)).isEqualTo(0L)
//    }
//
//    @Test
//    void shouldProcessDataEventFromUserNamespace(TestContext context) {
//        def async = context.async()
//
//        binding.functionHandler(function) {
//            it.payload()
//        }
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: user, from: 'from', function: function, toNamespace: namespace, to: 'to'))
//
//        binding.sendEvent(tenant, user, '_user', 'from', key, [:])
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.to"))) {
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldProcessDataEventFromAllUsersNamespace(TestContext context) {
//        def async = context.async()
//
//        binding.functionHandler(function) {
//            it.payload()
//        }
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: '_user', from: 'from', function: function, toNamespace: namespace, to: 'to'))
//
//        binding.sendEvent(tenant, user, '_user', 'from', key, [:])
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.to"))) {
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldProcessDataEventFromAllUsersNamespaceAndNotFromGroups(TestContext context) {
//        def async = context.async()
//
//        binding.functionHandler(function) {
//            it.payload()
//        }
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: '_user', from: 'from', function: function, toNamespace: toNamespace, to: to))
//
//        binding.sendEvent(tenant, user, '_someGroup', 'from', 'notUserKey', [:])
//        binding.sendEvent(tenant, user, '_user', 'from', key, [:])
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${toNamespace}.${to}"))) {
//            assertThat(it.key()).isEqualTo(key)
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldHandleFunctionException(TestContext context) {
//        def async = context.async()
//
//        binding.functionHandler(function) {
//            throw new RuntimeException('Ops!')
//        }
//        binding.addPipe(pipeId, new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: function, toNamespace: namespace, to: 'to'))
//
//        binding.sendEvent(tenant, user, namespace, 'from', key, [:])
//        binding.subscribe(new ConsumerConfig(topics("error.${pipeId}"))) {
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldHandleFunctionExceptionWithEmptyMessage() {
//        // Given
//        binding.functionHandler(function) {
//            throw new RuntimeException()
//        }
//        binding.addPipe(pipeId, pipe)
//
//        // When
//        binding.sendEvent(tenant, user, fromNamespace, from, key, [:])
//
//        // Then
//        await().untilAsserted {
//            assertThat(binding.countDataEventsErrors(pipeId)).isEqualTo(1)
//        }
//    }
//
//    // Functions tests
//
//    @Test
//    void shouldExecuteEnhancingFunctionByDefault(TestContext context) {
//        def async = context.async()
//        def pipeId = uuid()
//        def pipe = new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: 'myfunction', toNamespace: namespace, to: 'to')
//
//        binding.functionHandler(pipeId, pipe, false) {
//            it.payload()
//        }
//
//        binding.sendEvent(tenant, user, namespace, 'from', key, payload)
//
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.to"))) {
//            def result = fromJson(it.value()).enhanced["myfunction-${pipeId}"] as Map
//            assertThat(result).containsKey('foo')
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldExecuteReplacingFunction(TestContext context) {
//        def async = context.async()
//        def pipeId = uuid()
//        def pipe = new Pipe(tenant: tenant, fromNamespace: namespace, from: 'from', function: 'myfunction', toNamespace: namespace, to: 'to', configuration: [enhance: false])
//
//        binding.functionHandler(pipeId, pipe, false) {
//            it.payload()
//        }
//
//        binding.sendEvent(tenant, user, namespace, 'from', key, payload)
//
//        binding.subscribe(new ConsumerConfig(topics("data.${tenant}.${namespace}.to"))) {
//            def result = fromJson(it.value()) as Map
//            assertThat(result).containsKey('foo')
//            async.complete()
//        }
//    }

}
