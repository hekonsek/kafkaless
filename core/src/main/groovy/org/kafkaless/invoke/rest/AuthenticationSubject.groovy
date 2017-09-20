package org.kafkaless.invoke.rest

class AuthenticationSubject {

    private final String tenant

    private final String username

    AuthenticationSubject(String tenant, String username) {
        this.tenant = tenant
        this.username = username
    }

    String tenant() {
        tenant
    }

    String username() {
        username
    }

}
