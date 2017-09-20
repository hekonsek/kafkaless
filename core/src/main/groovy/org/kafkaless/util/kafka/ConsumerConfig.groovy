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

import com.google.common.collect.ImmutableMap
import groovy.transform.CompileStatic

import static org.kafkaless.util.Uuids.uuid

@CompileStatic
class ConsumerConfig {

    private final Topics topics

    private String errorTopic

    private String taskId

    private final Map<String, Object> consumerProperties = [:]

    // Constructors

    ConsumerConfig(Topics topics) {
        this.topics = topics
        taskId(uuid())
        autoOffsetReset('earliest')
        enableAutoCommit(false)
        groupdId(uuid())
    }

    Topics topics() {
        topics
    }

    ConsumerConfig taskId(String taskId) {
        this.taskId = taskId
        this
    }

    String taskId() {
        taskId
    }

    ConsumerConfig groupdId(String groupId) {
        consumerProperties.put('group.id', groupId)
        this
    }

    ConsumerConfig autoOffsetReset(String autoOffsetReset) {
        consumerProperties.put('auto.offset.reset', autoOffsetReset)
        this
    }

    ConsumerConfig enableAutoCommit(boolean enableAutoCommit) {
        consumerProperties['enable.auto.commit'] = "${enableAutoCommit}" as boolean
        this
    }

    String  errorTopic() {
        errorTopic
    }

    ConsumerConfig errorTopic(String errorTopic) {
        this.errorTopic = errorTopic
        this
    }

    ConsumerConfig metadataMaxAgeMs(int metadataMaxAgeMs) {
        consumerProperties['metadata.max.age.ms'] = metadataMaxAgeMs
        this
    }

    Map<String, Object> consumerProperties() {
        ImmutableMap.copyOf(consumerProperties)
    }

}
