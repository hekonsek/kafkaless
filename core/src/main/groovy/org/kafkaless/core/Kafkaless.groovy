package org.kafkaless.core

import org.kafkaless.util.Maps
import org.apache.commons.io.IOUtils
import org.kafkaless.core.api.EventCallback
import org.kafkaless.core.api.Event
import org.kafkaless.core.api.KafkalessOperations
import org.kafkaless.util.kafka.ConsumerConfig
import org.kafkaless.util.kafka.KafkaTemplate
import org.kafkaless.util.kafka.RequestReplyTemplate

import static com.google.common.base.Charsets.UTF_8
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent
import static org.kafkaless.util.kafka.Topics.Listed.topics

class Kafkaless implements KafkalessOperations {

    private static final PIPES_TOPIC = 'pipes'

    private final KafkaTemplate kafkaTemplate

    private final String tenant

    private final RequestReplyTemplate requestReplyTemplate

    Kafkaless(KafkaTemplate kafkaTemplate, String tenant) {
        this.kafkaTemplate = kafkaTemplate
        this.tenant = tenant
        this.requestReplyTemplate = new RequestReplyTemplate(kafkaTemplate, tenant)

        loadJsonDefinitions()
    }

    private loadJsonDefinitions() {
        def pipeDefinitionsJson = getClass().getResourceAsStream('/kafkaless.json')
        if(pipeDefinitionsJson == null) {
            return
        }
        def tenantPipes = (List<Map<String, Object>>) fromJson(IOUtils.toString(pipeDefinitionsJson, UTF_8)).pipes
        tenantPipes.each {
            def tenant = it.tenant as String
            def pipes = (List<Map<String, Object>>) it.pipes
            pipes.each {
                kafkaTemplate.sendEvent(pipesTopic(tenant), it.pipeId as String, Optional.of(mapEvent(it.pipe as Map)))
            }
        }
    }

    void functionHandler(String functionName, EventCallback eventCallback) {
        def pipesTopic = pipesTopic(tenant)
        kafkaTemplate.brokerAdmin().ensureTopicExists(pipesTopic)
        def requestReplyPipe = new Pipe(from: "${functionName}.requests", function: functionName)
        kafkaTemplate.sendEvent(pipesTopic, "${functionName}-requests", Optional.of(mapEvent(Maps.convert(requestReplyPipe))))

        kafkaTemplate.subscribe(new ConsumerConfig(topics(pipesTopic))) { pipeRecord ->
            if(pipeRecord.value() != null) {
                def pipe = fromJson(pipeRecord.value(), Pipe)
                if (pipe.function == functionName) {
                    (1..pipe.concurrencyLevel).each {
                        if(kafkaTemplate.isTaskStarted("${pipeRecord.key()}_${it}")) {
                            kafkaTemplate.stopConsumer("${pipeRecord.key()}_${it}")
                        }

                        startFunctionConsumer(tenant, pipeRecord.key(),  it, pipe, eventCallback)
                    }
                }
            } else {
                kafkaTemplate.listTasks().each {
                    if(it.startsWith("${pipeRecord.key()}_")) {
                        kafkaTemplate.stopConsumer(it)
                    }
                }
            }
        }
    }

    private void startFunctionConsumer(String tenant, String pipeId, int instanceNumber, Pipe pipe, EventCallback eventCallback) {
        def from = pipe.from
        def to = pipe.to

        ConsumerConfig config = new ConsumerConfig(topics("${tenant}.${from}"))
        kafkaTemplate.subscribe(config.groupdId(pipeId).taskId("${pipeId}_${instanceNumber}").errorTopic("${tenant}.${pipeId}.error")) {
            def event = fromJson(it.value(), Map)
            def metadata = event.metadata as Map
            def payload = event.payload as Map
            def result = eventCallback.onEvent(new Event(it.key(), metadata, Optional.of(payload)))
            def effectiveTo = to
            def clientId = result.metadata().clientId
            if(clientId != null) {
                effectiveTo = "responses.${clientId}"
            }
            kafkaTemplate.sendEvent("${tenant}.${effectiveTo}", it.key(), Optional.of(mapEvent([metadata: result.metadata(), payload: result.payload().orElse(null)])))
        }
    }

    Map<String, Object> invoke(String function, Map<String, Object> metadata, Map<String, Object> payload) {
        requestReplyTemplate.invoke(function, metadata, payload)
    }

    // Helpers

    private String pipesTopic(String tenant) {
        "${tenant}.${PIPES_TOPIC}"
    }

}