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
package kpipes.binding.util.docker

class ContainerBuilder {

    private final String image

    private String name

    private String net

    private Boolean cleanUp

    private Map<String, String> volumes = [:]

    private Map<String, String> environment = [:]

    private String[] arguments = []

    ContainerBuilder(String image) {
        this.image = image
    }

    Container build() {
        new Container(image, name, net, cleanUp, volumes, environment, arguments)
    }

    ContainerBuilder name(String name) {
        this.name = name
        this
    }

    ContainerBuilder net(String net) {
        this.net = net
        this
    }

    ContainerBuilder cleanUp(Boolean cleanUp) {
        this.cleanUp = cleanUp
        this
    }

    ContainerBuilder volumes(Map<String, String> volumes) {
        this.volumes = volumes
        this
    }

    ContainerBuilder environment(Map<String, String> environment) {
        this.environment = environment
        this
    }

    ContainerBuilder arguments(String... arguments) {
        this.arguments = arguments
        this
    }

}
