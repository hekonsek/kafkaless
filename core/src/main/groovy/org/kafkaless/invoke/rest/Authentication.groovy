package org.kafkaless.invoke.rest

import io.undertow.server.HttpServerExchange

interface Authentication {

    AuthenticationSubject authenticate(HttpServerExchange exchange)

}