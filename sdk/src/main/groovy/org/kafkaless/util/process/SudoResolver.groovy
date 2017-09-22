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
package org.kafkaless.util.process

import org.kafkaless.util.Config

/**
 * Resolves the proper way to execute command using sudo depending if user account is password-less or not.
 */
class SudoResolver {

    private final Config config

    SudoResolver(Config config) {
        this.config = config
    }

    SudoResolver() {
        this(new Config())
    }

    // Operations

    List<String> resolveSudo(Command command) {
        def commandSegments = command.command()
        def sudoPassword = command.sudoPassword()
        if(command.sudo() && config.configuration().getString('user.name') != 'root') {
            if(sudoPassword == null) {
                throw new IllegalStateException('Sudo access is required to execute the command. Please set up SUDO_PASSWORD environment variable or JVM system property.')
            } else if(sudoPassword.isEmpty()) {
                commandSegments.add(0, 'sudo')
            } else {
                commandSegments = ['/bin/bash', '-c', "echo '${sudoPassword}'| sudo -S ${commandSegments.join(' ')}".toString()]
            }
        }
        commandSegments
    }

}