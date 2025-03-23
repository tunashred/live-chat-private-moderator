package com.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import com.github.tunashred.moderator.Moderator;
import com.github.tunashred.moderator.ModeratorProducerConsumer;
import com.github.tunashred.moderator.WordsTrie;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaStreamTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setup() {
        final CountDownLatch initialLoadLatch = new CountDownLatch(1);
        Moderator moderator = new Moderator();
        WordsTrie wordsTrie = new WordsTrie();
        final String inputTopicName = "test-input-topic";
        Topology topology = ModeratorProducerConsumer.createTopology(inputTopicName, wordsTrie, moderator, initialLoadLatch);

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "kafka-stream-test");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void clientMessageProcessing() throws JsonProcessingException {
        // each group chat has its own topic named after it
        final String outputTopicName = "group-chat-dummy-1";
        outputTopic = testDriver.createOutputTopic(outputTopicName, new StringDeserializer(), new StringDeserializer());

        MessageInfo dummy_message_1 = new MessageInfo(
                new GroupChat(outputTopicName),
                new User("user-dummy-1"),
                "this is a message"
        );
        String serialized_message_1 = MessageInfo.serialize(dummy_message_1);

        MessageInfo dummy_message_2 = new MessageInfo(
                new GroupChat(outputTopicName),
                new User("user-dummy-2"),
                "a message this is"
        );
        String serialized_message_2 = MessageInfo.serialize(dummy_message_2);

        // produce
        inputTopic.pipeInput(dummy_message_1.getGroupChat().getChatID(), serialized_message_1);
        inputTopic.pipeInput(dummy_message_2.getGroupChat().getChatID(), serialized_message_2);

        // consume
        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();

        assertEquals(2, records.size());
        assertTrue(outputTopic.isEmpty());

        MessageInfo deserializedFirst = MessageInfo.deserialize(records.get(0).value);
        MessageInfo deserializedSecond = MessageInfo.deserialize(records.get(1).value);
        assertEquals(dummy_message_1, deserializedFirst);
        assertEquals(dummy_message_2, deserializedSecond);
    }
}
