package org.kafkaless.util.process

import org.kafkaless.util.Config
import org.assertj.core.api.Assertions
import org.junit.Test

import java.util.concurrent.ExecutionException

import static java.io.File.createTempFile

class DefaultProcessManagerTest {

    // Collaborators fixtures

    def processManager = new DefaultProcessManager(new SudoResolver(new Config()))

    // Tests

    @Test
    void shouldBeAbleToExecuteEcho() {
        def canExecuteEcho = processManager.canExecute(Command.cmd('echo'))
        Assertions.assertThat(canExecuteEcho).isTrue()
    }

    @Test
    void shouldNotBeAbleToExecuteRandomCommand() {
        def canExecuteEcho = processManager.canExecute(Command.cmd('invalidCommand'))
        Assertions.assertThat(canExecuteEcho).isFalse()
    }

    @Test
    void shouldRunEcho() {
        def output = processManager.execute(Command.cmd('echo', 'foo'))
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

    @Test
    void shouldHandleInvalidCommand() {
        try {
            processManager.execute(Command.cmd('invalidCommand'))
        } catch (ProcessExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(IOException.class)
            return
        }
        Assertions.fail('Expected process exception')
    }

    @Test
    void shouldRunEchoAsynchronously() {
        def output = processManager.executeAsync(Command.cmd('echo', 'foo'))
        Assertions.assertThat(output.get()).isEqualTo(['foo'])
    }

    @Test
    void shouldHandleInvalidAsynchronousCommand() {
        try {
            processManager.executeAsync(Command.cmd('invalidCommand')).get()
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ProcessExecutionException.class)
            return
        }
        Assertions.fail('Expected process exception')
    }

    @Test
    void shouldParseCommandWithSpaces() {
        def output = processManager.execute(Command.cmd('echo foo'))
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

    @Test
    void shouldParseCommandWithDoubleSpaces() {
        def output = processManager.execute(Command.cmd('echo  foo'))
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

    @Test
    void shouldParseCommandWithNewLines() {
        def output = processManager.execute(Command.cmd('echo\nfoo'))
        Assertions.assertThat(output).isEqualTo(['foo'])
    }

    @Test
    void shouldChangeWorkingDirectory() {
        // Given
        def tempFile = createTempFile('KPipes', 'test')
        def command = new CommandBuilder('ls').workingDirectory(tempFile.parentFile).build()

        // When
        def output = processManager.execute(command)

        // Then
        Assertions.assertThat(output).contains(tempFile.name)
    }

}
