package com.accelerate_experience.storm.rabbitmq.trident;

import com.accelerate_experience.storm.rabbitmq.Declarator;
import com.accelerate_experience.storm.rabbitmq.MessageScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * RabbitMq Trident Spout.
 */
public class TridentRabbitMqSpout implements ITridentSpout<RabbitMqBatch> {
    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";
    public static final int DEFAULT_BATCH_SIZE = 1000;
    private static final long serialVersionUID = 1L;

    private String name;
    private static int nameIndex = 1;
    private String streamId;
    private Map consumerMap;
    private Declarator declarator;
    private MessageScheme scheme;

    public TridentRabbitMqSpout(Scheme scheme, String streamId, Map consumerMap) {
        this(MessageScheme.Builder.from(scheme), null,new Declarator.NoOp(), streamId,consumerMap);
    }

    public TridentRabbitMqSpout(MessageScheme scheme, final TopologyContext context,
                                Declarator declarator, String streamId, Map consumerMap) {
        this.scheme = scheme;
        this.declarator = declarator;
        this.streamId = streamId;
        this.consumerMap = consumerMap;
        this.name = "RabbitMQSpout" + (nameIndex++);
    }

    @Override
    public Map getComponentConfiguration() {
        return consumerMap;
    }

    @Override
    public BatchCoordinator<RabbitMqBatch> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new RabbitMqBatchCoordinator(name);
    }

    @Override
    public Emitter<RabbitMqBatch> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new RabbitMqTridentEmitter(scheme, context, declarator, streamId, consumerMap);
    }

    @Override
    public Fields getOutputFields() {
        return this.scheme.getOutputFields();
    }
}
