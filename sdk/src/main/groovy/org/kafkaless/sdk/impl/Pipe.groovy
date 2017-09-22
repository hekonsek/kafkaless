package org.kafkaless.sdk.impl

class Pipe {

    String from

    String function

    String to

    int concurrencyLevel = 10

    Map<String, Object> configuration = [:]

}