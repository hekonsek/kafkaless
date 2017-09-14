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
package org.kafkaless.core.api

class EventContext {

    private final String key

    private final Map<String, Object> event

    private final Map<String, Object> metadata

    EventContext(String key, Map<String, Object> event, Map<String, Object> metadata) {
        this.key = key
        this.event = event
        this.metadata = metadata
    }

    String key() {
        key
    }

    Map<String, Object> event() {
        event
    }

    Map<String, Object> metadata() {
        metadata
    }

}