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
package kpipes.binding.util

import org.junit.Test

import static kpipes.binding.util.Maps.*
import static org.assertj.core.api.Assertions.assertThat
import static org.assertj.core.api.Assertions.fail

class MapsTest {

    @Test
    void shouldValidateNullValueAsBlank() {
        try {
            notBlankString([foo: null], 'foo')
        } catch (Exception e) {
            assertThat(e.message).contains('Expected to find not blank')
            return
        }
        fail('Validation not executed.')
    }

    @Test
    void shouldNoEntryAsBlank() {
        try {
            notBlankString([:], 'foo')
        } catch (Exception e) {
            assertThat(e.message).contains('Expected to find not blank')
            return
        }
        fail('Validation not executed.')
    }

    @Test
    void shouldValidateNullValueAsNullPojo() {
        try {
            notNullPojo([foo: null], 'foo', Map)
        } catch (Exception e) {
            assertThat(e.message).contains('Expected to find not null')
            return
        }
        fail('Validation not executed.')
    }

    @Test
    void shouldValidateNullEntryAsNullPojo() {
        try {
            notNullPojo([:], 'foo', Map)
        } catch (Exception e) {
            assertThat(e.message).contains('Expected to find not null')
            return
        }
        fail('Validation not executed.')
    }

    // Resolvers tests

    @Test
    void shouldResolveValue() {
        def resolvedValue = resolve([foo: 'bar'], String, 'foo')
        assertThat(resolvedValue).isEqualTo('bar')
    }

    @Test
    void shouldResolveValueFromEmbeddedMap() {
        def resolvedValue = resolve([envelope: [foo: 'bar']], String, 'foo')
        assertThat(resolvedValue).isEqualTo('bar')
    }

    @Test
    void shouldNotResolveNullValueAsNotBlank() {
        try {
            resolveNotBlankString([foo: null], 'foo')
        } catch (Exception e) {
            assertThat(e.message).contains('Expected to find not blank')
            return
        }
        fail('Validation not executed.')
    }

    @Test
    void shouldResolveIntFromEmbeddedMap() {
        def resolvedValue = resolveNotNullInt([envelope: [foo: 666]], 'foo')
        assertThat(resolvedValue).isEqualTo(666)
    }

}
