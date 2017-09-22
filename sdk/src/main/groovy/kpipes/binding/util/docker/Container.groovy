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
package kpipes.binding.util.docker

import com.google.common.collect.ImmutableList
import groovy.transform.ToString

/**
 * Represents CommandLineDocker container to be created.
 */
@ToString
class Container {

    private final String image

    private final String name

    private final String net

    private final Boolean cleanUp

    private final Map<String, String> volumes

    private final Map<String, String> environment

    private final String[] arguments

    Container(String image, String name, String net, Boolean cleanUp, Map<String, String> volumes, Map<String, String> environment, String[] arguments) {
        this.image = image
        this.name = name
        this.net = net
        this.cleanUp = cleanUp
        this.volumes = volumes
        this.environment = environment
        this.arguments = arguments
    }

    static Container container(String image, String name) {
        new Container(image, name, null, null, [:], [:])
    }

    static Container container(String image) {
        new Container(image, null, null, null, [:], [:])
    }

    // Getters

    String image() {
        image
    }

    String name() {
        name
    }

    String net() {
        net
    }
    
    Boolean cleanUp() {
        cleanUp
    }

    Map<String, String> volumes() {
        volumes
    }

    Map<String, String> environment() {
        return environment
    }

    List<String> arguments() {
        ImmutableList.copyOf(arguments)
    }

}
