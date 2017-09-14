package org.kafkaless.core

import kpipes.binding.util.Maps
import org.apache.commons.io.IOUtils
import org.kafkaless.core.api.EventCallback
import org.kafkaless.core.api.EventContext
import org.kafkaless.core.api.KafkalessOperations
import org.kafkaless.util.kafka.ConsumerConfig
import org.kafkaless.util.kafka.KafkaTemplate

import static com.google.common.base.Charsets.UTF_8
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent
import static org.kafkaless.util.kafka.Topics.Listed.topics

class Kafkaless implements KafkalessOperations {

    private final KafkaTemplate kafkaTemplate

    private final String tenant

    Kafkaless(KafkaTemplate kafkaTemplate, String tenant) {
        this.kafkaTemplate = kafkaTemplate
        this.tenant = tenant

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
                kafkaTemplate.sendEvent("${tenant}.functions", it.pipeId as String, Optional.of(mapEvent(it.pipe as Map)))
            }
        }
    }

    void functionHandler(String functionName, EventCallback eventCallback) {
        def requestReplyPipe = new Pipe(from: "${functionName}.requests", function: functionName)
        kafkaTemplate.sendEvent("${tenant}.functions", "${functionName}-requests", Optional.of(mapEvent(Maps.convert(requestReplyPipe))))

        kafkaTemplate.subscribe(new ConsumerConfig(topics("${tenant}.functions"))) {
            if(it.value() != null) {
                if(kafkaTemplate.isTaskStarted(it.key())) {
                    kafkaTemplate.stopConsumer(it.key())
                }

                def pipe = fromJson(it.value(), Pipe)
                if (pipe.function == functionName) {
                    startFunctionHandler(tenant, it.key(), pipe, eventCallback)
                }
            } else {
                kafkaTemplate.stopConsumer(it.key())
            }
        }
    }

    void startFunctionHandler(String tenant, String pipeId, Pipe pipe, EventCallback eventCallback) {
        def from = pipe.from
        def to = pipe.to

        ConsumerConfig config = new ConsumerConfig(topics("${tenant}.${from}"))
        kafkaTemplate.subscribe(config.groupdId(pipeId).errorTopic("${tenant}.${pipeId}.error")) {
            def event = fromJson(it.value(), Map)
            def metadata = event.metadata as Map
            def payload = event.payload as Map
            def result = eventCallback.onEvent(new EventContext(it.key(), payload, metadata))
            def effectiveTo = to
            def clientId = metadata.clientId as String
            if(clientId != null) {
                effectiveTo = "responses.${clientId}"
            }
            kafkaTemplate.sendEvent("${tenant}.${effectiveTo}", it.key(), Optional.of(mapEvent([metadata: result.metadata(), payload: result.event()])))
        }
    }

}