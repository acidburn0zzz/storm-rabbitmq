package com.accelerate_experience.storm.rabbitmq.trident;

import com.accelerate_experience.storm.rabbitmq.Declarator;
import com.accelerate_experience.storm.rabbitmq.ErrorReporter;
import com.accelerate_experience.storm.rabbitmq.Message;
import com.accelerate_experience.storm.rabbitmq.MessageScheme;
import com.accelerate_experience.storm.rabbitmq.RabbitMqConsumer;
import com.accelerate_experience.storm.rabbitmq.config.ConsumerConfig;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The RabbitMQTridentEmitter class listens for incoming messages and stores them in a blocking
 * queue. On each invocation of emit, the queued messages are emitted as a batch.
 */
public class RabbitMqTridentEmitter implements Emitter<RabbitMqBatch> {

    private final Logger log = LoggerFactory.getLogger(RabbitMqTridentEmitter.class);

    private int maxBatchSize = 3;
    private MessageScheme scheme = null;
    private transient RabbitMqConsumer consumer;
    private boolean active;
    private String streamId;

    private RotatingMap<Long, List<Message>> batchMessageMap = null; // Maps transaction ids

    private long rotateTimeMillis;

    private long lastRotate;

    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

    public RabbitMqTridentEmitter(
        MessageScheme scheme,
        final TopologyContext context,
        Declarator declarator,
        String streamId,
        Map consumerMap
    ) {
        batchMessageMap = new RotatingMap<Long, List<Message>>(maxBatchSize);
        ConsumerConfig consumerConfig = ConsumerConfig.getFromStormConfig(consumerMap);

        ErrorReporter reporter = new ErrorReporter() {

            private final Logger errorReporterLog = LoggerFactory.getLogger(ErrorReporter.class);

            @Override
            public void reportError(Throwable error) {
                errorReporterLog.warn("Error occurred", error);
            }
        };

        this.scheme = scheme;
        consumer = loadConsumer(declarator, reporter, consumerConfig);
        scheme.open(consumerMap, context);
        consumer.open();
        maxBatchSize = Integer.parseInt(consumerMap.get(MAX_BATCH_SIZE_CONF).toString());
        active = true;
        rotateTimeMillis = 1000L * Integer.parseInt(consumerMap.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        lastRotate = System.currentTimeMillis();
    }

    protected RabbitMqConsumer loadConsumer(Declarator declarator, ErrorReporter reporter, ConsumerConfig config) {
        return new RabbitMqConsumer(
            config.getConnectionConfig(),
            config.getPrefetchCount(),
            config.getQueueName(),
            config.isRequeueOnFail(),
            declarator,
            reporter
        );
    }

    @Override
    public void success(TransactionAttempt tx) {

        @SuppressWarnings("unchecked")
        List<Message> messages = (List<Message>) batchMessageMap.remove(tx.getTransactionId());

        if (messages != null) {
            if (!messages.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Success for batch with transaction id " + tx.getTransactionId() + "/"
                        + tx.getAttemptId() + " StreamId " + streamId);
                }
            }

            for (Message msg : messages) {
                Long messageId = Long.MIN_VALUE;
                try {
                    messageId = getDeliveryTag(msg);
                    // acking is important
                    if (messageId instanceof Long) {
                        consumer.ack((Long) messageId);
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("Acknowledged message " + messageId);
                    }
                } catch (Exception exc) {
                    log.warn("Failed to acknowledge message " + messageId, exc);
                }
            }
        } else {
            log.info("No messages found in batch with transaction id " + tx.getTransactionId() + "/"
                + tx.getAttemptId());
        }
    }

    @Override
    public void close() {
        try {
            log.info("Closing consumer connection.");
            consumer.close();
            scheme.close();
        } catch (Exception exc) {
            log.warn("Error closing consumer connection.", exc);
        }
    }

    @Override
    public void emitBatch(TransactionAttempt tx, RabbitMqBatch coordinatorMeta, TridentCollector collector) {
        long now = System.currentTimeMillis();
        if (now - lastRotate > rotateTimeMillis) {
            Map<Long, List<Message>> failed = batchMessageMap.rotate();
            for (Long id : failed.keySet()) {
                log.warn("TIMED OUT batch with transaction id " + id + " for " + streamId);
                fail(id, failed.get(id));
            }
            lastRotate = now;
        }

        if (batchMessageMap.containsKey(tx.getTransactionId())) {
            log.warn("FAILED duplicate batch with transaction id " + tx.getTransactionId() + "/"
                + tx.getAttemptId() + " for " + streamId);
            fail(tx.getTransactionId(), batchMessageMap.get(tx.getTransactionId()));
        }

        if (!active) {
            return;
        }

        int emitted = 0;
        Message message;

        List<Message> batchMessages = new ArrayList<Message>();

        while (emitted < maxBatchSize && (message = consumer.nextMessage()) != Message.NONE) {
            List<Object> tuple = extractTuple(message, collector);
            if (!tuple.isEmpty()) {
                batchMessages.add(message);
                collector.emit(tuple);
                emitted += 1;
            }
        }

        if (!batchMessages.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Emitting batch with transaction id " + tx.getTransactionId() + "/" + tx.getAttemptId()
                    + " and size " + batchMessages.size() + " for " + streamId);
            }
        } else {
            log.trace("No items to acknowledge for batch with transaction id " + tx.getTransactionId() + "/"
                + tx.getAttemptId() + " for " + streamId);
        }
        batchMessageMap.put(tx.getTransactionId(), batchMessages);
    }

    /**
     * Fail a batch with the given transaction id. This is called when a batch is timed out, or a new
     * batch with a matching transaction id is emitted. Note that the current implementation does
     * nothing - i.e. it discards messages that have been failed.
     *
     * @param transactionId The transaction id of the failed batch
     * @param messages The list of messages to fail.
    */
    private void fail(Long transactionId, List<Message> messages) {
        if (log.isDebugEnabled()) {
            log.debug("Failure for batch with transaction id " + transactionId + " for " + streamId);
        }

        if (messages != null) {
            for (Message msg : messages) {
                try {
                    Long msgId = getDeliveryTag(msg);

                    if (msgId instanceof Long) {
                        consumer.fail((Long) msgId);
                    }

                    if (log.isTraceEnabled()) {
                        log.trace("Failed message " + msgId);
                    }
                } catch (Exception e) {
                    log.warn("Could not identify failed message ", e);
                }
            }
        } else {
            log.warn("Failed batch has no messages with transaction id " + transactionId);
        }
    }

    private List<Object> extractTuple(Message message, TridentCollector collector) {
        try {
            List<Object> tuple = scheme.deserialize(message);

            if (tuple != null && !tuple.isEmpty()) {
                return tuple;
            }
        } catch (Exception e) {
            collector.reportError(e);
        }

        return Collections.emptyList();
    }

    protected long getDeliveryTag(Message message) {
        return ((Message.DeliveredMessage) message).getDeliveryTag();
    }
}
