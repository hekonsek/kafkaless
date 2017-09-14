package kpipes.binding.util

import org.junit.Test

import static org.assertj.core.api.Assertions.assertThat

class ConfigTest {

    @Test
    void shouldReadOptionUsingCommonsConfig() {
        def config = new Config('--foo=bar').configuration()
        def optionValue = config.getString('foo')
        assertThat(optionValue).isEqualTo('bar')
    }

    @Test
    void shouldResolveServiceHostFromArgs() {
        def fooHost = new Config('--FOO_SERVICE_HOST=baz').serviceHost('FOO')
        assertThat(fooHost).isEqualTo('baz')
    }

    @Test
    void shouldResolveDefaultServiceHost() {
        def fooHost = new Config().serviceHost('FOO')
        assertThat(fooHost).isEqualTo('localhost')
    }

    @Test
    void shouldIgnoreArgWithoutEqualCharacter() {
        new Config('--foo')
    }

}