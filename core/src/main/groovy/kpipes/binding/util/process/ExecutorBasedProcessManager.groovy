package kpipes.binding.util.process

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.Callable
import java.util.concurrent.Future

import static java.util.Arrays.asList
import static java.util.concurrent.Executors.newCachedThreadPool

/**
 * Base process manager using JDK Executor Framework for handling asynchronous command invocations.
 */
abstract class ExecutorBasedProcessManager implements ProcessManager {

    protected final Logger log = LoggerFactory.getLogger(getClass())

    private final executor = newCachedThreadPool()

    boolean canExecute(Command command) {
        try {
            execute(command)
            true
        } catch (ProcessExecutionException e) {
            log.debug("Cannot execute command " + asList(command) + " because of:", e)
            false
        }
    }

    @Override
    Future<List<String>> executeAsync(Command command) {
        executor.submit({execute(command)} as Callable<List<String>>)
    }

    void close() {
        executor.shutdown()
    }

    static String[] command(String command) {
        command.split(/\s+(?=([^"]*"[^"]*")*[^"]*$)/) // don't split values by whitespaces inside " "
    }

}