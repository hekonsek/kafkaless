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
package org.kafkaless.sdk

import org.kafkaless.util.Config
import org.kafkaless.sdk.impl.DefaultKafkaless
import org.kafkaless.util.kafka.KafkaTemplate

class KafkalessBuilder {

    private final Config config

    KafkalessBuilder(String... args) {
        this.config = new Config(args)
    }

    Kafkaless build() {
        def kafkaHost = config.serviceHost('KAFKA')
        def kafkaPort = config.servicePort('KAFKA', 9092)
        def zooKeeperHost = config.serviceHost('ZOOKEEPER')
        def zooKeeperPort = config.servicePort('ZOOKEEPER', 2181)
        def kafkaTemplate = new KafkaTemplate(kafkaHost, kafkaPort, zooKeeperHost, zooKeeperPort)

        def tenant = config.configuration().getString('TENANT', 'default')

        new DefaultKafkaless(kafkaTemplate, tenant)
    }

}
