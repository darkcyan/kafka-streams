package com.example.risk;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiskStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RiskStreamsUncaughtExceptionHandler.class);

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        if (exception instanceof Error) {
            // JVM-level errors (OOM, StackOverflow) — shut down this instance and
            // let the container/orchestrator restart it
            log.error("Fatal JVM error on Streams thread, shutting down client", exception);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }

        // For all other exceptions: replace the failed thread and keep the app running.
        // Kafka rebalances partitions within this instance; no other pods are affected.
        log.error("Uncaught exception on Streams thread, replacing thread", exception);
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
