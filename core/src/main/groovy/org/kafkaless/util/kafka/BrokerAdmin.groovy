package org.kafkaless.util.kafka

import groovy.transform.CompileStatic
import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.utils.ZKStringSerializer$
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.Logger
import scala.collection.JavaConversions

import java.util.concurrent.Callable

import static kafka.admin.AdminUtils.fetchAllTopicConfigs
import static org.awaitility.Awaitility.await
import static org.slf4j.LoggerFactory.getLogger

@CompileStatic
class BrokerAdmin {

    private static final Logger LOG = getLogger(BrokerAdmin)

    private final String zooKeeperHost

    private final int zooKeeperPort

    private final int defaultPartitionsNumber

    BrokerAdmin(String zooKeeperHost, int zooKeeperPort, int defaultPartitionsNumber) {
        this.zooKeeperHost = zooKeeperHost
        this.zooKeeperPort = zooKeeperPort
        this.defaultPartitionsNumber = defaultPartitionsNumber
    }

    void ensureTopicExists(Set<String> topics) {
        def zkClient = new ZkClient("${zooKeeperHost}:${zooKeeperPort}", Integer.MAX_VALUE, 10000, ZKStringSerializer$.MODULE$)
        ZkUtils zooKeeperUtils = ZkUtils.apply(zkClient, false)

        List<String> topicsCreated = []
        topics.each { topic ->
            try {
                if (!AdminUtils.topicExists(zooKeeperUtils, topic)) {
                    RackAwareMode mode = RackAwareMode.Disabled$.MODULE$
                    AdminUtils.createTopic(zooKeeperUtils, topic, defaultPartitionsNumber, 1, new Properties(), mode)
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
        def zkClient = new ZkClient("${zooKeeperHost}:${zooKeeperPort}", Integer.MAX_VALUE, 10000, ZKStringSerializer$.MODULE$)
        ZkUtils zooKeeperUtils = ZkUtils.apply(zkClient, false)
        JavaConversions.mapAsJavaMap(fetchAllTopicConfigs(zooKeeperUtils)).collect { it.key }
    }

}