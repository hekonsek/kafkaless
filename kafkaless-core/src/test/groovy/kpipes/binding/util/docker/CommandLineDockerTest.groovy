package kpipes.binding.util.docker

import org.assertj.core.api.Assertions
import org.junit.Test

class CommandLineDockerTest {

    @Test
    void shouldRunAsDaemon() {
        // Given
        def container = new ContainerBuilder('image').build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, true)

        // Then
        Assertions.assertThat(command).contains(' -d ')
    }

    @Test
    void shouldNotRunAsDaemon() {
        // Given
        def container = new ContainerBuilder('image').build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, false)

        // Then
        Assertions.assertThat(command).doesNotContain(' -d ')
    }

    @Test
    void shouldNotMountVolumes() {
        // Given
        def container = new ContainerBuilder('image').build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, false)

        // Then
        Assertions.assertThat(command).doesNotContain(' -v ')
    }

    @Test
    void shouldMountVolumes() {
        // Given
        def container = new ContainerBuilder('image').volumes([foo: 'bar']).build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, false)

        // Then
        Assertions.assertThat(command).contains(' -v foo:bar ')
    }

    @Test
    void shouldPassEnvironment() {
        // Given
        def container = new ContainerBuilder('image').environment([foo: 'bar']).build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, false)

        // Then
        Assertions.assertThat(command).contains(' -e foo=bar ')
    }

    @Test
    void shouldNotRunCleanUpWhenRunAsDaemon() {
        // Given
        def container = new ContainerBuilder('image').cleanUp(true).build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, true)

        // Then
        Assertions.assertThat(command).contains(' -d ')
        Assertions.assertThat(command).doesNotContain(' --rm ')
    }

    @Test
    void shouldRunCleanUp() {
        // Given
        def container = new ContainerBuilder('image').cleanUp(true).build()

        // When
        def command = CommandLineDocker.buildRunCommand(container, false)

        // Then
        Assertions.assertThat(command).contains(' --rm ')
    }
}
