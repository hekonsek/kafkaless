package kpipes.binding.kafka

import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Ignore
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner)
@Ignore
class KafkaEventTemplateTest {
    
//    // Fixtures
//
//    def binding = new KafkaBindingBuilder().build()
//
//    def template = binding.@template
//
//    def tenant = uuid()
//
//    def user = uuid()
//
//    def namespace = uuid()
//
//    def topic = uuid()
//
//    def streamName = uuid()
//
//    def streamTopic = "data.${tenant}.${namespace}.${streamName}"
//
//    def key = uuid()
//
//    def payload = [foo: uuid()]
//
//    def eventBytes = jsonBytes(payload)
//
//    @Test
//    void shouldValidateNullTopic() {
//        try {
//            template.sendEvent(null, key, Optional.of(eventBytes))
//        } catch (NullPointerException e) {
//            assertThat(e.message).isEqualTo('Topic cannot be blank.')
//            return
//        }
//        fail('Null validation expected.')
//    }
//
//    @Test
//    void shouldReceiveEvent(TestContext context) {
//        // Given
//        def async = context.async()
//
//        // When
//        template.sendEvent(topic, key, Optional.of(eventBytes))
//
//        binding.subscribe(new ConsumerConfig(topics(topic))) {
//            // Then
//            def payload = fromJson(it.value())
//            assertThat(payload).isEqualTo(this.payload)
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldReceiveEventByRegex(TestContext context) {
//        def async = context.async()
//
//        template.sendEvent(topic, key, Optional.of(eventBytes))
//
//        binding.subscribe(new ConsumerConfig(new Topics.Regex(/${topic.substring(0, 5)}.+/))) {
//            async.complete()
//        }
//    }
//
//    @Test
//    void shouldTailEvents() {
//        // When
//        template.sendEvent(topic, key, Optional.of(eventBytes))
//
//        // Then
//        await().untilAsserted {
//            def events = template.tailEvents(topic, 10)
//            assertThat(events).hasSize(1)
//            assertThat(events.first().key()).isEqualTo(key)
//            def event = fromJson(events.first().value())
//            assertThat(payload).isEqualTo(payload)
//        }
//    }

}