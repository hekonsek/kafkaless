package kpipes.binding

import org.kafkaless.core.Pipe
import org.kafkaless.core.api.EventCallback
import org.kafkaless.core.api.EventContext
import org.kafkaless.util.kafka.BrokerAdmin
import org.kafkaless.util.kafka.ConsumerConfig
import org.kafkaless.util.kafka.RecordCallback
import org.kafkaless.util.kafka.Topics
import kpipes.binding.view.KeyValueMaterializedView
import org.apache.commons.lang3.Validate
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes
import org.slf4j.Logger

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean

import static java.util.Optional.empty
import static java.util.concurrent.Executors.newCachedThreadPool
import static org.kafkaless.util.kafka.Topics.Listed.topics
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Json.jsonString
import static org.kafkaless.util.Maps.convert
import static kpipes.binding.util.Reflections.isContainer
import static kpipes.binding.util.Reflections.isJavaLibraryType
import static org.kafkaless.util.Uuids.uuid
import static org.awaitility.Awaitility.await
import static org.slf4j.LoggerFactory.getLogger

class KafkaBinding {

    // Static members

    private final static Logger LOG = getLogger(KafkaBinding)

    private final String kafkaHost

    private final int kafkaPort

    private final Producer producer

    private final BrokerAdmin brokerAdmin

//    private final KafkaEventTemplate template

    private final ExecutorService executor = newCachedThreadPool()

    private final Map<String, AtomicBoolean> stopRequests = [:]

    private KeyValueMaterializedView<Pipe> pipesView

    // Constructors

    KafkaBinding(String kafkaHost, int kafkaPort, String zooKeeperHost, int zooKeeperPort) {
        this.kafkaHost = kafkaHost
        this.kafkaPort = kafkaPort

        Map<String, Object> producerConfig = [:]
        producerConfig['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        producerConfig['key.serializer'] = StringSerializer.name
        producerConfig['value.serializer'] = BytesSerializer.name
        producer = new KafkaProducer(producerConfig)

        brokerAdmin = new BrokerAdmin(zooKeeperHost, zooKeeperPort, 1)

//        template = new KafkaEventTemplate(kafkaHost, kafkaPort, zooKeeperHost, zooKeeperPort)
    }

    // Generic operations
//
////    void subscribe(ConsumerConfig consumerConfig, RecordCallback recordCallback) {
//        def topics = consumerConfig.topics()
////        if (topics instanceof Topics.Listed) {
//            brokerAdmin.ensureTopicExists(topics.topics() as Set)
//        }
//
//        def consumerProperties = new HashMap<>(consumerConfig.consumerProperties())
//        consumerProperties['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
//        consumerProperties['key.deserializer'] = StringDeserializer.name
//        consumerProperties['value.deserializer'] = BytesDeserializer.name
//
//        def consumer = new KafkaConsumer<String, Bytes>(consumerProperties)
//        if (topics instanceof Topics.Listed) {
//            consumer.subscribe(topics.topics())
//        } else if (topics instanceof Topics.Regex) {
//            consumer.subscribe(topics.regex(), new ConsumerRebalanceListener() {
//                @Override
//                void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                    LOG.debug('Revoked partitions: {}', partitions)
//                }
//
//                @Override
//                void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                    LOG.debug('Assigned partitions: {}', partitions)
//                }
//            })
//        } else {
//            throw new IllegalArgumentException("Unknown topics specification type: ${topics.class}")
//        }
//
//        def taskId = consumerConfig.taskId()
//        stopRequests.put(taskId, new AtomicBoolean(false))
//        executor.submit {
//            while (!stopRequests[taskId].get()) {
//                try {
//                    def events = consumer.poll(5000)
//                    def iterator = events.iterator()
//                    while (iterator.hasNext()) {
//                        def record = iterator.next()
//                        try {
//                            recordCallback.onRecord(record)
//                        } catch (Exception e) {
//                            def eventBytes = (record.value() as Bytes).get()
//
//                            def errorWriter = new ByteArrayOutputStream(5 * 1024)
//                            def errorPrinter = errorWriter.newPrintWriter()
//                            def message = e.message != null ? e.message : ''
//                            errorPrinter.println(message.bytes)
//                            e.printStackTrace(errorPrinter)
//                            errorPrinter.close()
//                            errorWriter.close()
//
//                            def errorMessage = "${message}:\n${new String(errorWriter.toByteArray())}\n${new String(eventBytes)}"
//                            def errorTopic = consumerConfig.errorTopic() != null ? consumerConfig.errorTopic() : "error.${record.topic()}"
//                            brokerAdmin.ensureTopicExists(errorTopic)
//
//                            producer.send(new ProducerRecord(errorTopic, record.key(), new Bytes(errorMessage.bytes)))
//                        }
//                        consumer.commitSync()
//                    }
//                    Thread.sleep(100)
//                } catch (WakeupException e) {
//                    stopRequests[taskId].set(true)
//                    consumer.close()
//                }
//            }
//        }
//    }

    void stopConsumer(String taskId) {
        if (stopRequests.containsKey(taskId)) {
            stopRequests[taskId].set(true)
        }
    }

    // Data events

    void deleteDataEvent(String tenant, String user, String namespace, String streamName, String key) {
        sendEvent(tenant, user, namespace, streamName, key, empty())
    }

    Map<String, Map<String, Object>> tailDataEvents(String tenant, String user, String namespace, String streamName, int number) {
        def topic = "data.${tenant}.${effectiveNamespace(user, namespace)}.${streamName}"
        template.tailEvents(topic, number).inject([:]){ map, record -> map[record.key()] = fromJson(record.value()); map }
    }



    // Data events error

    Map<String, Map<String, String>> tailDataEventsErrors(String pipeId, int number) {
        def topic = "error.${pipeId}"
        template.tailEvents(topic, number).inject([:]){ map, record -> map[record.key()] = new String(record.value().get()); map }
    }

    long countDataEventsErrors(String pipeId) {
        def topic = "error.${pipeId}"
        brokerAdmin.ensureTopicExists(topic)

        def consumerConfig = new HashMap<>(new ConsumerConfig(topics(topic)).consumerProperties())
        consumerConfig['bootstrap.servers'] = "${kafkaHost}:${kafkaPort}" as String
        consumerConfig.put('key.deserializer', StringDeserializer.name)
        consumerConfig.put('value.deserializer', BytesDeserializer.name)

        KafkaConsumer<String, Bytes> consumer = null
        try {
            consumer = new KafkaConsumer<String, Bytes>(consumerConfig)
            def partitions = consumer.partitionsFor(topic).collect {
                new TopicPartition(topic, it.partition())
            }
            consumer.endOffsets(partitions).collect { it.value }.inject(0) { total, partitionSize -> total + partitionSize }
        } finally {
            consumer.close()
        }
    }

    // Service operations events

    void sendRequestEvent(String tenant, String user, String clientId, String requestId, String service, String operation, Map<String, Object> event) {
        Validate.notBlank(tenant, 'Request tenant cannot be blank.')
        Validate.notBlank(user, 'Request user cannot be blank.')
        Validate.notBlank(clientId, "Request's client ID cannot be blank.")
        Validate.notBlank(service, 'Request service cannot be blank.')
        Validate.notBlank(operation, 'Request operation cannot be blank.')
        Validate.notNull(event, "Request event can't be null.")

        def topic = "service.request.${service}.${operation}"
        def serializedEvent = new Bytes(jsonString([metadata: [tenant: tenant, user: user, clientId: clientId], event: event]).bytes)
        brokerAdmin.ensureTopicExists(topic)
        producer.send(new ProducerRecord(topic, requestId, serializedEvent)).get()
    }

    void subscribeForResponseEvents(String taskId, String clientId, RecordCallback recordCallback) {
        def topic = "service.response.${clientId}"
        brokerAdmin.ensureTopicExists(topic)
        subscribe(new ConsumerConfig(topics(topic)).autoOffsetReset('latest').taskId(taskId), recordCallback)
    }

    void subscribeForRequestEvents(String taskId, String service, String operation, RecordCallback recordCallback) {
        def topic = "service.request.${service}.${operation}"
        brokerAdmin.ensureTopicExists(topic)
        subscribe(new ConsumerConfig(topics(topic)).autoOffsetReset('latest').taskId(taskId), recordCallback)
    }

    void sendResponseEvent(String clientId, String requestId, Object response) {
        def topic = "service.response.${clientId}"
        def serializedEvent = new Bytes(jsonString([response: response]).bytes)
        brokerAdmin.ensureTopicExists(topic)
        producer.send(new ProducerRecord(topic, requestId, serializedEvent)).get()
    }

    void sendErrorResponseEvent(String clientId, String requestId, String response) {
        def topic = "service.response.${clientId}"
        def serializedEvent = new Bytes(jsonString([error: response]).bytes)
        brokerAdmin.ensureTopicExists(topic)
        producer.send(new ProducerRecord(topic, requestId, serializedEvent)).get()
    }

    void registerServiceOperation(String service, String operation, EventCallback eventCallback) {
        subscribeForRequestEvents(uuid(), service, operation) {
            def requestEvent = fromJson(it.value(), Map)
            def clientId = requestEvent.metadata.clientId as String
            def eventContext = new EventContext(it.key(), requestEvent.event as Map, requestEvent.metadata as Map)
            try {
                def result = eventCallback.onEvent(eventContext)
                if(result == null) {
                    result = 'OK'
                } else if(result instanceof Optional) {
                    result = result.orElse(null)
                }
                sendResponseEvent(clientId, it.key(), result)
            } catch (Exception e) {
                LOG.debug('Error while processing request event:', e)
                sendErrorResponseEvent(clientId, it.key(), e.message)
            }
        }
    }

    def <T> T executeOperation(String tenant, String user, String service, String operation, Map<String, Object> event, Class<T> responseType) {
        def clientId = uuid()
        def requestId = uuid()
        def responseConsumerTaskId = uuid()
        try {
            Map<String, Object> response = null
            subscribeForResponseEvents(responseConsumerTaskId, clientId) {
                response = fromJson(it.value())
            }
            sendRequestEvent(tenant, user, clientId, requestId, service, operation, event)
            await().until({ response != null } as Callable<Boolean>)
            response.response as T
        } finally {
            stopConsumer(responseConsumerTaskId)
        }
    }

    // Pipes

    void materializePipes(KeyValueMaterializedView<Pipe> pipeMaterializedView) {
        pipesView = pipeMaterializedView
        subscribe(new ConsumerConfig(topics('pipe'))) {
            if(it.value() == null) {
                pipesView.remove('admin', 'admin', it.key())
            } else {
                def pipe = fromJson(it.value(), Pipe)
                pipesView.put('admin', 'admin', it.key(), pipe)
            }
        }
    }

    void addPipe(String pipeId, Pipe pipe) {
        Validate.notBlank(pipeId, 'Pipe ID cannot be blank.')

        template.sendEvent('pipe', pipeId, Optional.of(jsonString(pipe).bytes))
    }

    void deletePipe(String pipeId) {
        Validate.notBlank(pipeId, 'Pipe ID cannot be blank.')

        template.sendEvent('pipe', pipeId, empty())
    }

    Map<String, Pipe> listPipes() {
        pipesView.list('admin', 'admin')
    }

    void registerFunction(String selector, boolean hasManualOutput, EventCallback eventCallback) {
        subscribe(new ConsumerConfig(topics('pipe'))) {
            if(it.value() != null) {
                def pipe = fromJson(it.value(), Pipe)
                if (pipe.function == selector) {
                    startFunctionPipe(it.key(), pipe, hasManualOutput, eventCallback)
                }
            } else {
                stopConsumer(it.key())
            }
        }
    }

    void registerFunction(String selector, EventCallback eventCallback) {
        registerFunction(selector, false, eventCallback)
    }

    // Functions

    void startFunctionPipe(String pipeId, Pipe pipe, boolean hasManualOutput, EventCallback eventCallback) {
        def tenant = pipe.tenant
        def fromNamespace = pipe.fromNamespace
        def from = pipe.from
        def toNamespace = pipe.toNamespace
        def to = pipe.to

        ConsumerConfig config
        if(fromNamespace == '_user') {
            config = new ConsumerConfig(new Topics.Regex(/data\.${tenant}\.(?!_).+?\.${from}/))
            config.metadataMaxAgeMs(5000)
        } else {
            config = new ConsumerConfig(topics("data.${tenant}.${fromNamespace}.${from}"))
        }
        subscribe(config.groupdId(pipeId).errorTopic("error.${pipeId}")) {
            def event = fromJson(it.value(), Map)
            def metadata = [tenant: tenant, fromNamespace: fromNamespace, from: from, toNamespace: toNamespace, to: to, configuration: pipe.configuration]
            def result = eventCallback.onEvent(new EventContext(it.key(), new LinkedHashMap<String, Object>(event), metadata))

            if(hasManualOutput) {
                return
            }

            Map<String, Object> processedEvent
            def enhance = pipe.configuration.enhance as Boolean
            if(enhance == null || enhance) {
                def results = event.enhanced as Map
                if(results == null) {
                    results = [:]
                    event.enhanced = results
                }
                results["${pipe.function}-${pipeId}"] = result
                processedEvent = event
            } else {
                def isNonMapContainer = isContainer(result.getClass()) && !(result instanceof Map)
                if(isJavaLibraryType(result.getClass()) || isNonMapContainer) {
                    processedEvent = [event: result]
                } else {
                    processedEvent = convert(result)
                }
            }
            sendEvent(tenant, null, toNamespace, to, it.key(), processedEvent)
        }
    }

}