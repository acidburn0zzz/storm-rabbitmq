package com.accelerate_experience.storm.rabbitmq.trident;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bare implementation of a BatchCoordinator, retuning a null RabbitMq object.
 */
public class RabbitMqBatchCoordinator implements BatchCoordinator<RabbitMqBatch> {

    private final String name;

    private final Logger log = LoggerFactory.getLogger(RabbitMqBatchCoordinator.class);

    public RabbitMqBatchCoordinator(String name) {
        this.name = name;
        log.info("Created batch coordinator for " + name);
    }

    @Override
    public RabbitMqBatch initializeTransaction(long txId, RabbitMqBatch prevMetadata, RabbitMqBatch curMetadata) {
        if (log.isDebugEnabled()) {
            log.debug("Initialize transaction " + txId + " for " + name);
        }
        return null;
    }

    @Override
    public void success(long txId) {

    }

    @Override
    public boolean isReady(long txId) {
        return true;
    }

    @Override
    public void close() {

    }
}
