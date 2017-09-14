package kpipes.binding

import kpipes.binding.util.Config

class KafkaBindingBuilder {

    private final Config config

    KafkaBindingBuilder(String... args) {
        this.config = new Config(args)
    }

    KafkaBinding build() {
        def zooKeeperHost = config.serviceHost('ZOOKEEPER')
        def zooKeeperPort = config.servicePort('ZOOKEEPER', 2181)
        def kafkaHost = config.serviceHost('KAFKA')
        def kafkaPort = config.servicePort('KAFKA', 9092)

        new KafkaBinding(kafkaHost, kafkaPort, zooKeeperHost, zooKeeperPort)
    }

}
