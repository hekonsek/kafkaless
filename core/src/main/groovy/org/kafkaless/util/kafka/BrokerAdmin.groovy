package org.kafkaless.util.kafka

import groovy.transform.CompileStatic
import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.utils.ZKStringSerializer$
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.Logger

import java.util.concurrent.Callable

import static kafka.admin.AdminUtils.createTopic
import static kafka.admin.AdminUtils.fetchAllTopicConfigs
import static org.awaitility.Awaitility.await
import static org.slf4j.LoggerFactory.getLogger
import static scala.collection.JavaConversions.mapAsJavaMap

@CompileStatic
class BrokerAdmin {

    private static final Logger LOG = getLogger(BrokerAdmin)

    private final String zooKeeperHost

    private final int zooKeeperPort

    private final int defaultPartitionsNumber

    private final ZkClient zkClient

    BrokerAdmin(String zooKeeperHost, int zooKeeperPort, int defaultPartitionsNumber) {
        this.zooKeeperHost = zooKeeperHost
        this.zooKeeperPort = zooKeeperPort
        this.defaultPartitionsNumber = defaultPartitionsNumber

        zkClient = new ZkClient("${zooKeeperHost}:${zooKeeperPort}", Integer.MAX_VALUE, 10000, ZKStringSerializer$.MODULE$)
    }

    // Operations

    void ensureTopicExists(Set<String> topics) {
        def zooKeeperUtils = ZkUtils.apply(zkClient, false)

        List<String> topicsCreated = []
        topics.each { topic ->
            try {
                if (!AdminUtils.topicExists(zooKeeperUtils, topic)) {
                    def mode = RackAwareMode.Disabled$.MODULE$
                    def topicProperties = new Properties()
                    topicProperties.put('cleanup.policy', 'compact')
                    createTopic(zooKeeperUtils, topic, defaultPartitionsNumber, 1, topicProperties, mode)
                    topicsCreated << topic
                }
            } catch (TopicExistsException e) {
                LOG.debug(e.message)
            }
        }
        topicsCreated.each { topic ->
            await().until({AdminUtils.topicExists(zooKeeperUtils, topic)} as Callable<Boolean>)
        }
    }

    void ensureTopicExists(String... topics) {
        ensureTopicExists(topics.toList().toSet())
    }

    List<String> topics() {
        ZkUtils zooKeeperUtils = ZkUtils.apply(zkClient, false)
        mapAsJavaMap(fetchAllTopicConfigs(zooKeeperUtils)).collect { it.key }
    }

}