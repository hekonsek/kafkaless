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

import kpipes.binding.util.docker.CommandLineDocker
import kpipes.binding.util.docker.ContainerBuilder
import kpipes.binding.util.process.DefaultProcessManager
import kpipes.binding.util.process.SudoResolver

import static org.kafkaless.util.Mavens.artifactVersionFromDependenciesProperties
import static org.kafkaless.util.Uuids.uuid

final class DockerizedKafka {

    static docker = new CommandLineDocker(new DefaultProcessManager(new SudoResolver()))

    static kafkalessVersion = artifactVersionFromDependenciesProperties('org.kafkaless', 'kafkaless-core').orElseThrow {
        new IllegalStateException('Cannot read Kafkaless version from Maven metadata.')
    }

    private DockerizedKafka() {
    }

    static ensureKafkaIsRunning() {
        docker.startService(new ContainerBuilder("kafkaless/zookeeper:${kafkalessVersion}").name(uuid()).net('host').build())
        docker.startService(new ContainerBuilder("kafkaless/kafka:${kafkalessVersion}").name(uuid()).net('host').build())
        Thread.sleep(5000)
    }

}