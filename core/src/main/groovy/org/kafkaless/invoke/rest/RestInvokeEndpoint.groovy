package org.kafkaless.invoke.rest

import io.undertow.Undertow
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.server.handlers.BlockingHandler
import org.apache.commons.io.IOUtils
import org.kafkaless.endpoint.management.ManagementService

import static com.google.common.base.Charsets.UTF_8
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Json.jsonString

class RestInvokeEndpoint {

    private final ManagementService managementService

    private final Authentication authentication

    RestInvokeEndpoint(ManagementService managementService, Authentication authentication) {
        this.managementService = managementService
        this.authentication = authentication
    }

    void start() {
        Undertow.builder()
                .addHttpListener(8081, '0.0.0.0')
                .setHandler(new BlockingHandler(new HttpHandler() {
            @Override
            void handleRequest(HttpServerExchange exchange) {
                def subject = authentication.authenticate(exchange)
                def uri = exchange.requestURI.substring(1)
                def path = uri.split(/\//)
                def payload = fromJson(IOUtils.toString(exchange.inputStream, UTF_8))
                def response = managementService.invoke(subject.tenant(), path[0], [metadata: [:], payload: payload])
                exchange.getResponseSender().send(jsonString(response.payload))
            }
        })).build().start()
    }

}
