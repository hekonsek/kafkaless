package org.kafkaless.endpoint.management

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.apache.commons.io.IOUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.kafkaless.core.Kafkaless
import org.kafkaless.util.kafka.KafkaTemplate

import static io.vertx.core.Vertx.vertx
import static org.assertj.core.api.Assertions.assertThat
import static org.kafkaless.util.Json.fromJson
import static org.kafkaless.util.Json.jsonString
import static org.kafkaless.util.Uuids.uuid

@RunWith(VertxUnitRunner)
class ManagementRestEndpointTest {

    static def template = new KafkaTemplate('localhost', 9092, 'localhost', 2181)

    static def managementEndpoint = new ManagementRestEndpoint(new ManagementService(template))

    static {
        managementEndpoint.start()
    }

    def tenant = uuid()

    def functionName = uuid()

    def topic = uuid()

    def event = [metadata: [metaNumber: 666], payload: [foo: 'bar']]

    // Tests

    @Test
    void shouldCountEmptyTopic() {
        def countResponse = IOUtils.toString(new URL("http://localhost:8080/countEvents/${tenant}/${topic}"))
        def count = fromJson(countResponse).count as int
        assertThat(count).isEqualTo(0)
    }

    @Test
    void shouldSaveEvent(TestContext context) {
        def async = context.async()
        vertx().createHttpClient().post(8080, 'localhost',"/saveEvent/${tenant}/${topic}/key").handler {
            it.bodyHandler {
                def response = fromJson(it.bytes)
                assertThat(response.status).isEqualTo('OK')

                def countResponse = IOUtils.toString(new URL("http://localhost:8080/countEvents/${tenant}/${topic}"))
                def count = fromJson(countResponse).count as int
                assertThat(count).isEqualTo(1)

                async.complete()
            }
        }.end(jsonString(event))
    }

    @Test
    void shouldCountErrorEvent(TestContext context) {
        def async = context.async()
        vertx().createHttpClient().post(8080, 'localhost',"/saveEvent/${tenant}/${topic}.error/key").handler {
            it.bodyHandler {
                def response = fromJson(it.bytes)
                assertThat(response.status).isEqualTo('OK')

                def countResponse = IOUtils.toString(new URL("http://localhost:8080/countErrorEvents/${tenant}/${topic}"))
                def count = fromJson(countResponse).count as int
                assertThat(count).isEqualTo(1)

                async.complete()
            }
        }.end(jsonString(event))
    }

    @Test
    void shouldInvokeFunction(TestContext context) {
        def async = context.async()

        new Kafkaless(template, tenant).functionHandler(functionName) {
            it
        }
        Thread.sleep(5000)

        vertx().createHttpClient().post(8080, 'localhost',"/invoke/${tenant}/${functionName}").handler {
            it.bodyHandler {
                def response = fromJson(it.bytes)
                assertThat(response.metadata).isNotNull()
                async.complete()
            }
        }.end(jsonString(event))
    }

}