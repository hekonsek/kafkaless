package org.kafkaless.util.kafka

import static kpipes.binding.util.Maps.convert

interface Event {

    static class MapEvent implements Event {

        private final Map<String, Object> event

        MapEvent(Map<String, Object> event) {
            this.event = event
        }

        static MapEvent mapEvent(Map<String, Object> event) {
            new MapEvent(event)
        }

        static MapEvent mapEvent(Object event) {
            new MapEvent(convert(event))
        }

        Map<String, Object> event() {
            return event
        }

    }

    static class BinaryEvent implements Event {

        private final byte[] event

        BinaryEvent(byte[] event) {
            this.event = event
        }

        byte[] event() {
            return event
        }

    }

}