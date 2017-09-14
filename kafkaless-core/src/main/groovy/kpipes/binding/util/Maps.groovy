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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.Validate

class Maps {

    private static final MAPPER = new ObjectMapper()

    static <T> T convert(Object object, Class<T> to) {
        MAPPER.convertValue(object, to)
    }

    static Map<String, Object> convert(Object object) {
        convert(object, Map)
    }

    static String notBlankString(Map<String, Object> map, String key) {
        Validate.notBlank(map[key] as String, "Expected to find not blank string in %s under key '%s'.", map, key)
    }

    static <T> T notNullPojo(Map<String, Object> map, String key, Class<T> type) {
        def message = "Expected to find not null pojo of type '%s' in %s under key '%s'."
        Validate.notNull(MAPPER.convertValue(map[key], type), message, type.name, map, key)
    }

    // Resolvers

    static <T> T resolve(Map<String, Object> map, Class<T> type, String... keys) {
        for(String key in keys) {
            def value = map[key]
            if (value != null) {
                return value
            }
            def embeddedMaps = map.values().findAll { it instanceof Map } as List<Map>
            for (embeddedMap in embeddedMaps) {
                def embeddedValue = resolve(embeddedMap, type, key)
                if (embeddedValue != null) {
                    return embeddedValue
                }
            }
        }
        null
    }

    static String resolveNotBlankString(Map<String, Object> map, String... keys) {
        Validate.notBlank(resolve(map, String, keys) as String, "Expected to find not blank string in %s under key '%s'.", map, keys)
    }

    static int resolveNotNullInt(Map<String, Object> map, String... keys) {
        Validate.notNull(resolve(map, Integer, keys) as Integer, "Expected to find not null integer in %s under key '%s'.", map, keys)
    }

}