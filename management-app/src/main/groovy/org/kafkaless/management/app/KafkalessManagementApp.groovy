package org.kafkaless.management.app

import org.kafkaless.endpoint.management.ManagementRestEndpoint
import org.kafkaless.endpoint.management.ManagementService
import org.kafkaless.util.kafka.KafkaTemplate

class KafkalessManagementApp {

    public static void main(String[] args) {
        def kafkaTemplate = new KafkaTemplate('localhost', 9092, 'localhost', 2181)
        def managementService = new ManagementService(kafkaTemplate)
        new ManagementRestEndpoint(managementService).start()
    }

}