package kpipes.binding.util.process

import kpipes.binding.util.Config
import org.assertj.core.api.Assertions
import org.junit.Test

class SudoResolverTest {

    def config = new Config()

    def sudoResolver = new SudoResolver(config)

    @Test
    void nonRootWithNonEmptyPasswordShouldUseSudoInPipe() {
        // Given
        System.setProperty('user.name', 'notRoot')
        def command = CommandBuilder.sudo('echo foo').sudoPassword('nonEmptyPassword').build()

        // When
        def enhancedCommand = sudoResolver.resolveSudo(command)

        // Then
        Assertions.assertThat(enhancedCommand.last()).contains('sudo')
    }

    @Test
    void nonRootWithBlankPasswordShouldUseSudoInPipe() {
        // Given
        System.setProperty('user.name', 'notRoot')
        def command = CommandBuilder.sudo('echo foo').sudoPassword(' ').build()

        // When
        def enhancedCommand = sudoResolver.resolveSudo(command)

        // Then
        Assertions.assertThat(enhancedCommand.last()).contains('sudo')
    }

    @Test
    void nonRootWithEmptyPasswordShouldUseSudoPrefix() {
        // Given
        System.setProperty('user.name', 'notRoot')
        def command = CommandBuilder.sudo('echo foo').sudoPassword('').build()

        // When
        def enhancedCommand = sudoResolver.resolveSudo(command)

        // Then
        Assertions.assertThat(enhancedCommand.first()).isEqualTo('sudo')
    }

    @Test
    void rootShouldNotUseSudo() {
        // Given
        System.setProperty('user.name', 'root')
        def command = CommandBuilder.sudo('echo foo').build()

        // When
        def enhancedCommand = sudoResolver.resolveSudo(command)

        // Then
        Assertions.assertThat(enhancedCommand).isEqualTo(['echo', 'foo'])
    }

}
