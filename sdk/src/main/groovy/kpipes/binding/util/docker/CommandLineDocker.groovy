package kpipes.binding.util.docker

import com.fasterxml.jackson.databind.ObjectMapper
import org.kafkaless.util.process.Command
import org.kafkaless.util.process.ProcessManager
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory

class CommandLineDocker implements Docker {

    private final static LOG = LoggerFactory.getLogger(CommandLineDocker.class)

    private final static MAPPER = new ObjectMapper()

    private final ProcessManager processManager

    // Constructor

    CommandLineDocker(ProcessManager processManager) {
        this.processManager = processManager
    }

    // Operations implementation

    @Override
    List<String> execute(Container container) {
        processManager.execute(Command.cmd(buildRunCommand(container, false)))
    }

    ServiceStartupResults startService(Container container) {
        Validate.notNull(container, 'Container cannot be null.')
        LOG.debug('About to execute container service: {}', container)

        Validate.notBlank(container.name(), 'Container service name must not be empty.')

        switch(status(container.name())) {
            case ContainerStatus.running: return ServiceStartupResults.alreadyRunning
            case ContainerStatus.created:
                processManager.execute(Command.cmd("docker start ${container.name()}"))
                return ServiceStartupResults.started
            case ContainerStatus.none:
                processManager.execute(Command.cmd(buildRunCommand(container, true)))
                return ServiceStartupResults.created
        }
    }

    ContainerStatus status(String name) {
        if (processManager.execute(Command.cmd("docker ps -a -f name=${name}")).size() > 1) {
            if (processManager.execute(Command.cmd("docker ps -f name=${name}")).size() > 1) {
                ContainerStatus.running
            } else {
                ContainerStatus.created
            }
        } else {
            ContainerStatus.none
        }
    }

    @Override
    void stop(String name) {
        Validate.notBlank(name, 'Container name cannot be blank.')
        LOG.debug('About to stop container: {}', name)

        processManager.execute(Command.cmd("docker stop ${name}"))
    }

    @Override
    InspectResults inspect(String containerId) {
        def commandOutput = processManager.execute(Command.cmd("docker inspect ${containerId}")).join(' ')
        def trimmedCommandOutput = commandOutput.substring(1, commandOutput.length() - 1)
        new InspectResults(MAPPER.readValue(trimmedCommandOutput, Map.class))
    }

    // Helpers

    static private String buildRunCommand(Container container, boolean daemon) {
        def command = 'docker run'
        if(daemon) {
            command += ' -d'
        } else {
            if (container.cleanUp()) {
                command += ' --rm'
            }
        }
        if(container.name() != null) {
            command += " --name=${container.name()}"
        }
        if(container.net() != null) {
            command += " --net=${container.net()} "
        }
        command += " ${container.volumes().inject('') { volumes, volume -> "${volumes} -v ${volume.key}:${volume.value}"}}"
        command += " ${container.environment().inject('') { environment, variable -> "${environment} -e ${variable.key}=${variable.value}"}}"
        command + " -t ${container.image()} ${container.arguments().join(' ')}"
    }

}
