package org.kafkaless.invoke.rest

import io.undertow.server.HttpServerExchange

class MockAuthentication implements Authentication {

    private final String tenant

    MockAuthentication(String tenant) {
        this.tenant = tenant
    }

    @Override
    AuthenticationSubject authenticate(HttpServerExchange exchange) {
        new AuthenticationSubject(tenant, 'user')
    }

}
