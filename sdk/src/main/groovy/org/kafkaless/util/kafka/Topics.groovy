package org.kafkaless.util.kafka

import groovy.transform.CompileStatic

import java.util.regex.Pattern

@CompileStatic
interface Topics {

    static class Listed implements Topics {

        private final List<String> topics

        Listed(List<String> topics) {
            this.topics = topics
        }

        static Listed topics(String... topics) {
            new Listed(topics as List)
        }

        List<String> topics() {
            topics
        }

    }

    static class Regex implements Topics {

        private final String regex

        Regex(String regex) {
            this.regex = regex
        }

        Pattern regex() {
            Pattern.compile(regex)
        }

    }

}