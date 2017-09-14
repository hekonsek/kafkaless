package kpipes.binding.util.process

import org.assertj.core.api.Assertions
import org.junit.Test

import static org.kafkaless.util.Uuids.uuid

class CommandTest {

    String command = uuid()

    // Tests

    @Test
    void shouldParseStringBySpace() {
        def command = CommandBuilder.cmd('foo bar').build()
        Assertions.assertThat(command.command()).isEqualTo(['foo', 'bar'])
    }

    // Sudo tests

    @Test
    void shouldCreateCommandWithSudoEnabled() {
        def command = CommandBuilder.sudo(command).build()
        Assertions.assertThat(command.sudo()).isTrue()
    }

    @Test
    void shouldParseCommandWithSudoEnabled() {
        def command = CommandBuilder.sudo('foo bar').build()
        Assertions.assertThat(command.sudo()).isTrue()
        Assertions.assertThat(command.command()).isEqualTo(['foo', 'bar'])
    }

    @Test
    void shouldCreateCommandWithSudoDisabled() {
        def command = CommandBuilder.cmd(command).build()
        Assertions.assertThat(command.sudo()).isFalse()
    }

    // toString() tests

    @Test
    void toStringShouldIncludeWorkingDirectory() {
        def commandToString = CommandBuilder.cmd(command).workingDirectory(new File('/foo')).build().toString()
        Assertions.assertThat(commandToString).contains('workingDirectory:/foo')
    }

    @Test
    void toStringShouldIncludeNullWorkingDirectory() {
        def commandToString = CommandBuilder.cmd(command).build().toString()
        Assertions.assertThat(commandToString).contains('workingDirectory:null')
    }

}
