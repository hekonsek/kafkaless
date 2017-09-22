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
package org.kafkaless.util

import org.apache.commons.configuration2.CompositeConfiguration
import org.apache.commons.configuration2.Configuration
import org.apache.commons.configuration2.EnvironmentConfiguration
import org.apache.commons.configuration2.MapConfiguration
import org.apache.commons.configuration2.SystemConfiguration

class Config {

    private final Configuration configuration

    Config(String... args) {
        configuration = new CompositeConfiguration()
        configuration.setThrowExceptionOnMissing(true)

        def argsConfig = args.findAll{ it.matches(/--.+\=.+/) }.inject([:]){ map, arg ->
            def parsedArg = arg.substring(2).split(/\=/)
            map[parsedArg[0]] = parsedArg[1]
            map
        }
        configuration.addConfiguration(new MapConfiguration(argsConfig))
        configuration.addConfiguration(new SystemConfiguration())
        configuration.addConfiguration(new EnvironmentConfiguration())
    }

    Configuration configuration() {
        configuration
    }

    // Extra configuration accessor

    String serviceHost(String service) {
        configuration.getString("${service.toUpperCase()}_SERVICE_HOST", 'localhost')
    }

    int servicePort(String service, int defaultPort) {
        configuration.getInt("${service.toUpperCase()}_SERVICE_PORT", defaultPort)
    }

}