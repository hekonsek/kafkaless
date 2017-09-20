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

import org.junit.Test

import static org.assertj.core.api.Assertions.assertThat
import static org.kafkaless.util.Reflections.isPojo
import static org.kafkaless.util.Reflections.wrappableAwareInstanceOf

class ReflectionsTest {

    @Test
    void shouldDetectThatIntIsNotString() {
        assertThat(wrappableAwareInstanceOf(int.class, String.class)).isFalse()
    }

    @Test
    void shouldDetectThatIntIsInt() {
        assertThat(wrappableAwareInstanceOf(int.class, int.class)).isTrue()
    }

    @Test
    void shouldDetectThatIntegerIsInteger() {
        assertThat(wrappableAwareInstanceOf(Integer.class, Integer.class)).isTrue()
    }

    @Test
    void shouldDetectThatIntIsInteger() {
        assertThat(wrappableAwareInstanceOf(int.class, Integer.class)).isTrue()
    }

    @Test
    void shouldDetectThatIntegerIsInt() {
        assertThat(wrappableAwareInstanceOf(Integer.class, int.class)).isTrue()
    }

    @Test
    void shouldDetectThatIntIsNotPojo() {
        assertThat(isPojo(10.class)).isFalse()
    }

    @Test
    void shouldDetectThatMapIsNotPojo() {
        assertThat(isPojo(Map.class)).isFalse()
    }

    @Test
    void shouldDetectThatListIsNotPojo() {
        assertThat(isPojo(List.class)).isFalse()
    }

    @Test
    void shouldDetectThatObjectIsNotPojo() {
        assertThat(isPojo(Object)).isFalse()
    }

    @Test
    void shouldDetectPojo() {
        assertThat(isPojo(Pojo.class)).isTrue()
    }

    static class Pojo {}

}