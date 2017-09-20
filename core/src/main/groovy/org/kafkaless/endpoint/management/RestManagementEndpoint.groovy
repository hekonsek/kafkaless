package org.kafkaless.endpoint.management

import com.fasterxml.jackson.databind.ObjectMapper
import io.undertow.Undertow
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.server.handlers.BlockingHandler
import org.apache.commons.io.IOUtils

import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Json.jsonString
import static org.kafkaless.util.kafka.Event.MapEvent.mapEvent

class RestManagementEndpoint {

    private final ManagementService managementService

    RestManagementEndpoint(ManagementService managementService) {
        this.managementService = managementService
    }

    void start() {
        Undertow.builder()
                .addHttpListener(8080, '0.0.0.0')
                .setHandler(new BlockingHandler(new HttpHandler() {
            @Override
            void handleRequest(final HttpServerExchange exchange) {
                def uri = exchange.requestURI.substring(1)
                if(uri.startsWith('saveEvent/')) {
                    def path = uri.split(/\//)
                    def tenant = path[1]
                    def topic = path[2]
                    def key = path[3]
                    def event = fromJson(IOUtils.toString(exchange.inputStream))
                    managementService.saveEvent(tenant, topic, key, Optional.of(mapEvent(event)))
                    exchange.getResponseSender().send(new ObjectMapper().writeValueAsString([status: 'OK']))
                } else if(uri.startsWith('countEvents/')) {
                    def path = uri.split(/\//)
                    def tenant = path[1]
                    def topic = path[2]
                    def count = managementService.countEvents(tenant, topic)
                    exchange.getResponseSender().send(new ObjectMapper().writeValueAsString([count: count]))
                } else if(uri.startsWith('countErrorEvents/')) {
                    def path = uri.split(/\//)
                    def tenant = path[1]
                    def topic = path[2]
                    def count = managementService.countErrorEvents(tenant, topic)
                    exchange.getResponseSender().send(new ObjectMapper().writeValueAsString([count: count]))
                } else if(uri.startsWith('invoke/')) {
                    def path = uri.split(/\//)
                    def tenant = path[1]
                    def function = path[2]
                    def event = fromJson(IOUtils.toString(exchange.inputStream))
                    def response = managementService.invoke(tenant, function, event)
                    exchange.getResponseSender().send(jsonString(response))
                }
            }
        })).build().start()
    }

}
