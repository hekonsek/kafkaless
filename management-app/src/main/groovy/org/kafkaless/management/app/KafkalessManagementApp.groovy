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
package org.kafkaless.management.app

import org.kafkaless.endpoint.management.ManagementService
import org.kafkaless.endpoint.management.RestManagementEndpoint
import org.kafkaless.invoke.rest.MockAuthentication
import org.kafkaless.invoke.rest.RestInvokeEndpoint
import org.kafkaless.util.kafka.KafkaTemplate

class KafkalessManagementApp {

    static void main(String... args) {
        def kafkaTemplate = new KafkaTemplate('localhost', 9092, 'localhost', 2181)
        def managementService = new ManagementService(kafkaTemplate)

        new RestManagementEndpoint(managementService).start()

        def authentication = new MockAuthentication('default')
        new RestInvokeEndpoint(managementService, authentication).start()
    }

}