package com.accelerate_experience.storm.rabbitmq;

import org.apache.storm.tuple.Tuple;

/**
 * Simple extension of {@link RabbitMqBolt} that provides the ability to determine whether a message should be published
 * based on the input tuple.
 *
 * This class is sort of an SPI meaning that it is meant to be subclassed
 * and the method {@link ConditionalPublishingRabbitMqBolt#shouldPublish}
 * to be overridden with the custom decision logic.
 */
public class ConditionalPublishingRabbitMqBolt extends RabbitMqBolt {

    public ConditionalPublishingRabbitMqBolt(TupleToMessage scheme) {
        super(scheme);
    }

    public ConditionalPublishingRabbitMqBolt(TupleToMessage scheme, Declarator declarator) {
        super(scheme, declarator);
    }

    @Override
    public void execute(final Tuple tuple) {
        if (shouldPublish(tuple)) {
            publish(tuple);
        }
        acknowledge(tuple);
    }

    protected boolean shouldPublish(Tuple tuple) {
        return true;
    }
}
