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
package org.kafkaless.endpoint.management

import com.google.common.cache.Cache
import org.apache.kafka.common.utils.Bytes
import org.kafkaless.util.kafka.ConsumerConfig
import org.kafkaless.util.kafka.Event
import org.kafkaless.util.kafka.KafkaTemplate
import org.kafkaless.util.kafka.Topics

import java.util.concurrent.Callable

import static com.google.common.cache.CacheBuilder.newBuilder
import static java.util.concurrent.TimeUnit.MINUTES
import static org.awaitility.Awaitility.await
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Uuids.uuid
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent

class ManagementService {

    private final static NEW_CLIENT_PROPAGATION_TIME = 5000

    private final KafkaTemplate template

    private final clientId = uuid()

    private final Cache<String, Bytes> responses = newBuilder().expireAfterWrite(2, MINUTES).build()

    ManagementService(KafkaTemplate template) {
        this.template = template

        registerRequestClient()
    }

    private registerRequestClient() {
        template.subscribe(new ConsumerConfig(new Topics.Regex(/.+?\.responses\.${clientId}/)).metadataMaxAgeMs(NEW_CLIENT_PROPAGATION_TIME).autoOffsetReset('latest')) {
            responses.put(it.key(), it.value())
        }
    }

    // Operations

    void saveEvent(String tenant, String topic, String key, Optional<Event> event) {
        template.sendEvent("${tenant}.${topic}", key, event)
    }

    long countEvents(String tenant, String topic) {
        template.countDataEvents("${tenant}.${topic}")
    }

    long countErrorEvents(String tenant, String topic) {
        template.countDataEvents("${tenant}.${topic}.error")
    }

    Map<String, Object> invoke(String tenant, String function, Map<String, Object> event) {
        def responseTopic = "${tenant}.responses.${clientId}" as String
        if(!template.brokerAdmin().topics().contains(responseTopic)) {
            template.brokerAdmin().ensureTopicExists(responseTopic)
            Thread.sleep(NEW_CLIENT_PROPAGATION_TIME + 1)
        }
        def requestId = uuid()
        event.metadata.clientId = clientId
        saveEvent(tenant, "${function}.requests", requestId, Optional.of(mapEvent(event)))
        await().until({
            responses.getIfPresent(requestId) != null
        } as Callable<Boolean>)
        def response = fromJson(responses.getIfPresent(requestId) as Bytes)
        responses.invalidate(requestId)
        response
    }

}
