package com.accelerate_experience.storm.rabbitmq;

import static org.hamcrest.Matchers.is;

import org.apache.storm.spout.Scheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MessageSchemeBuilderTest {

    @Test
    public void fromDoesNotWrapMessageSchemes() {
        Scheme messageScheme = new MessageScheme() {
            @Override
            public void open(Map config, TopologyContext context) {
                // No-Op
            }

            @Override
            public void close() {
                // No-Op
            }

            @Override
            public List<Object> deserialize(Message message) {
                return null;
            }

            @Override
            public List<Object> deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public Fields getOutputFields() {
                return null;
            }
        };

        MessageScheme fromScheme = MessageScheme.Builder.from(messageScheme);

        Assert.assertThat(fromScheme, is(messageScheme));
    }

    @Test
    public void fromDoesWrapOtherSchemes() {
        Message testMessage = new Message(ByteBuffer.wrap("Test".getBytes()));

        Scheme scheme = new Scheme() {
            @Override
            public List<Object> deserialize(ByteBuffer byteBuffer) {
                return Arrays.<Object>asList("Hello1", "Hello2");
            }

            @Override
            public Fields getOutputFields() {
                return new Fields("testField1", "testField2");
            }
        };

        MessageScheme fromScheme = MessageScheme.Builder.from(scheme);

        // should not have any effect
        fromScheme.open(null, null);
        List<String> fields = fromScheme.getOutputFields().toList();
        Assert.assertThat(fields, is(Arrays.asList("testField1", "testField2")));

        List<Object> deserializedMessage = fromScheme.deserialize(testMessage);
        Assert.assertThat(deserializedMessage, is(Arrays.<Object>asList("Hello1", "Hello2")));

        // should not have any effect
        fromScheme.close();
    }


}