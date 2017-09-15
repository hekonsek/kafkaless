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
package org.kafkaless.util.kafka

import com.google.common.cache.Cache
import org.apache.kafka.common.utils.Bytes

import java.util.concurrent.Callable

import static com.google.common.cache.CacheBuilder.newBuilder
import static java.util.concurrent.TimeUnit.MINUTES
import static org.awaitility.Awaitility.await
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Uuids.uuid
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent
import static org.kafkaless.util.kafka.Topics.Listed.topics

class RequestReplyTemplate {

    private final KafkaTemplate template

    private final String tenant

    private final clientId = "requestReplyTemplate-${uuid()}" as String

    private final Cache<String, Bytes> responses = newBuilder().expireAfterWrite(2, MINUTES).build()

    RequestReplyTemplate(KafkaTemplate template, String tenant) {
        this.template = template
        this.tenant = tenant

        registerRequestClient()
    }

    private registerRequestClient() {
        template.subscribe(new ConsumerConfig(topics("${tenant}.responses.${clientId}".toString())).autoOffsetReset('latest')) {
            responses.put(it.key(), it.value())
        }
    }

    // Operations

    Map<String, Object> invoke(String function, Map<String, Object> metadata, Map<String, Object> payload) {
        def requestId = uuid()
        def oldClientId = metadata.clientId
        metadata.clientId = clientId
        template.sendEvent("${tenant}.${function}.requests", requestId, Optional.of(mapEvent([metadata: metadata, payload: payload])))
        await().until({
            responses.getIfPresent(requestId) != null
        } as Callable<Boolean>)
        def response = fromJson(responses.getIfPresent(requestId) as Bytes)
        responses.invalidate(requestId)
        if(oldClientId != null) {
            response.metadata.clientId = oldClientId
        }
        response
    }

}
