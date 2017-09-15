package org.kafkaless.util.kafka

import com.google.common.collect.MinMaxPriorityQueue
import groovy.transform.CompileStatic
import org.apache.commons.lang3.Validate
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean

import static java.lang.Math.max
import static java.util.concurrent.Executors.newCachedThreadPool
import static org.kafkaless.util.Json.jsonBytes
import static org.kafkaless.util.kafka.Topics.Listed.topics

@CompileStatic
class KafkaTemplate {

    private final String kafkaHost

    private final int kafkaPort

    private final Producer<String, Byte[]> producer

    private final BrokerAdmin brokerAdmin

    private final Map<String, AtomicBoolean> stopRequests = [:]

    private final ExecutorService executor = newCachedThreadPool()

    KafkaTemplate(String kafkaHost, int kafkaPort, String zooKeeperHost, int zooKeeperPort) {
        this.kafkaHost = kafkaHost
        this.kafkaPort = kafkaPort

        Map<String, Object> producerConfig = [:]
        producerConfig['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        producerConfig['key.serializer'] = StringSerializer.name
        producerConfig['value.serializer'] = BytesSerializer.name
        producer = new KafkaProducer(producerConfig)

        brokerAdmin = new BrokerAdmin(zooKeeperHost, zooKeeperPort, 25)
    }

    void sendEvent(String topic, String key, Optional<Event> event) {
        Validate.notBlank(topic, 'Topic cannot be blank.')
        Validate.notNull(event, 'Event cannot be null.')

        Bytes serializedEvent
        if(event.present) {
            if(event.get() instanceof Event.MapEvent) {
                def mapEvent = event.get() as Event.MapEvent
                serializedEvent = new Bytes(jsonBytes(mapEvent.event()))
            } else if(event.get() instanceof Event.BinaryEvent) {
                def binaryEvent = event.get() as Event.BinaryEvent
                serializedEvent = new Bytes(binaryEvent.event())
            } else {
                throw new IllegalArgumentException("Passed event is not an instance of ${Event.name}.")
            }
        } else {
            serializedEvent = null
        }

        brokerAdmin.ensureTopicExists(topic)
        producer.send(new ProducerRecord(topic, key, serializedEvent)).get()
    }

    long countDataEvents(String topic) {
        brokerAdmin.ensureTopicExists(topic)

        def consumerConfig = new HashMap<>(new ConsumerConfig(topics(topic)).consumerProperties())
        consumerConfig['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        consumerConfig.put('key.deserializer', StringDeserializer.name)
        consumerConfig.put('value.deserializer', BytesDeserializer.name)
        consumerConfig.put('enable.auto.commit', 'false')

        KafkaConsumer<String, Bytes> consumer = null
        try {
            consumer = new KafkaConsumer<String, Bytes>(consumerConfig)
            def partitions = consumer.partitionsFor(topic).collect {
                new TopicPartition(topic, it.partition())
            }
            consumer.endOffsets(partitions).collect { it.value }.inject(0L) { total, partitionSize -> total + partitionSize } as long
        } finally {
            consumer.close()
        }
    }

    List<ConsumerRecord<String, Bytes>> tailEvents(String topic, int number) {
        Validate.notBlank(topic, 'Topic cannot be blank.')
        Validate.isTrue(number > 0, 'Number of records to read must be positive.')

        brokerAdmin.ensureTopicExists(topic)

        def consumerConfig = new HashMap<>(new ConsumerConfig(topics(topic)).consumerProperties())
        consumerConfig['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        consumerConfig['key.deserializer'] = StringDeserializer.name
        consumerConfig['value.deserializer'] = BytesDeserializer.name
        consumerConfig['enable.auto.commit'] = 'false'

        KafkaConsumer<String, Bytes> consumer = null
        try {
            consumer = new KafkaConsumer<String, Bytes>(consumerConfig)
            def partitions = consumer.partitionsFor(topic).collect {
                new TopicPartition(topic, it.partition())
            }
            consumer.assign(partitions)
            consumer.endOffsets(partitions).each {
                consumer.seek(it.key, max(it.value - number, 0))
            }
            MinMaxPriorityQueue<ConsumerRecord<String, Bytes>> sortedEvents = MinMaxPriorityQueue.orderedBy(new Comparator<ConsumerRecord<String, Bytes>>() {
                @Override
                int compare(ConsumerRecord<String, Bytes> first, ConsumerRecord<String, Bytes> second) {
                    second.timestamp() - first.timestamp()
                }
            }).maximumSize(number).create()
            while(true) {
                def records = consumer.poll(1000)
                if (records.empty) {
                    return sortedEvents.toList()
                }
                sortedEvents.addAll(records.iterator())
            }
        } finally {
            consumer.close()
        }
    }

    void subscribe(ConsumerConfig consumerConfig, RecordCallback recordCallback) {
        def topics = consumerConfig.topics()
        if (topics instanceof Topics.Listed) {
            brokerAdmin.ensureTopicExists(topics.topics() as Set)
        }

        def consumerProperties = new HashMap<>(consumerConfig.consumerProperties())
        consumerProperties['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        consumerProperties['key.deserializer'] = StringDeserializer.name
        consumerProperties['value.deserializer'] = BytesDeserializer.name

        def consumer = new KafkaConsumer<String, Bytes>(consumerProperties)
        if (topics instanceof Topics.Listed) {
            consumer.subscribe(topics.topics())
        } else if (topics instanceof Topics.Regex) {
            consumer.subscribe(topics.regex(), new ConsumerRebalanceListener() {
                @Override
                void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                }
            })
        } else {
            throw new IllegalArgumentException("Unknown topics specification type: ${topics.class}")
        }

        def taskId = consumerConfig.taskId()
        stopRequests.put(taskId, new AtomicBoolean(false))
        executor.submit {
            while (!stopRequests[taskId].get()) {
                try {
                    def events = consumer.poll(5000)
                    def iterator = events.iterator()
                    while (iterator.hasNext()) {
                        def record = iterator.next()
                        try {
                            recordCallback.onRecord(record)
                        } catch (Exception e) {
                            def eventBytes = (record.value() as Bytes).get()

                            def errorWriter = new ByteArrayOutputStream(5 * 1024)
                            def errorPrinter = errorWriter.newPrintWriter()
                            def message = e.message != null ? e.message : ''
                            errorPrinter.println(message.bytes)
                            e.printStackTrace(errorPrinter)
                            errorPrinter.close()
                            errorWriter.close()

                            def errorMessage = "${message}:\n${new String(errorWriter.toByteArray())}\n${new String(eventBytes)}"
                            def errorTopic = consumerConfig.errorTopic() != null ? consumerConfig.errorTopic() : "error.${record.topic()}" as String
                            brokerAdmin.ensureTopicExists(errorTopic)

                            producer.send(new ProducerRecord(errorTopic, record.key(), new Bytes(errorMessage.bytes)))
                        }
                        consumer.commitSync()
                    }
                } catch (WakeupException e) {
                    stopRequests[taskId].set(true)
                    consumer.close()
                }
            }
        }
    }

    boolean isTaskStarted(String taskId) {
        stopRequests.containsKey(taskId) && !stopRequests[taskId].get()
    }

    void stopConsumer(String taskId) {
        if (stopRequests.containsKey(taskId)) {
            stopRequests[taskId].set(true)
        }
    }

    // Accessors

    BrokerAdmin brokerAdmin() {
        brokerAdmin
    }

}