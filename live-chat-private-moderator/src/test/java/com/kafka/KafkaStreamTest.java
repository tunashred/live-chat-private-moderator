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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.junit.jupiter.api.Assertions.*;

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

        MessageInfo dummyMessage1 = new MessageInfo(
                new GroupChat(outputTopicName),
                new User("user-dummy-1"),
                "this is a message"
        );
        String serializedMessage1 = MessageInfo.serialize(dummyMessage1);

        MessageInfo dummyMessage2 = new MessageInfo(
                new GroupChat(outputTopicName),
                new User("user-dummy-2"),
                "a message this is"
        );
        String serializedMessage2 = MessageInfo.serialize(dummyMessage2);

        // produce
        inputTopic.pipeInput(dummyMessage1.getGroupChat().getChatID(), serializedMessage1);
        inputTopic.pipeInput(dummyMessage2.getGroupChat().getChatID(), serializedMessage2);

        // consume
        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();

        assertEquals(2, records.size());
        assertTrue(outputTopic.isEmpty());

        MessageInfo deserializedFirst = MessageInfo.deserialize(records.get(0).value);
        MessageInfo deserializedSecond = MessageInfo.deserialize(records.get(1).value);
        assertEquals(dummyMessage1, deserializedFirst);
        assertEquals(dummyMessage2, deserializedSecond);
    }

    @Test
    void censorMessage() throws JsonProcessingException {
        final String outputTopicName = "group-chat-dummy-1";
        outputTopic = testDriver.createOutputTopic(outputTopicName, new StringDeserializer(), new StringDeserializer());

        // create dummy banned word
        String dummyBannedWord = "windows";
        TestInputTopic<String, String> bannedWordsTopic = testDriver.createInputTopic("banned-words", new StringSerializer(), new StringSerializer());
        bannedWordsTopic.pipeInput(dummyBannedWord, dummyBannedWord, 1000L);

        // produce message
        MessageInfo dummyMessage = new MessageInfo(
                new GroupChat(outputTopicName),
                new User("user-dummy-1"),
                "this message is about windows. all kinds of windows"
        );
        String serializedMessage1 = MessageInfo.serialize(dummyMessage);
        inputTopic.pipeInput(dummyMessage.getGroupChat().getChatID(), serializedMessage1, 2000L);

        // fetch banned word
        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");
        String bannedWord = bannedWordsStore.get(dummyBannedWord);
        assertEquals(dummyBannedWord, bannedWord);

        // consume processed message
        TestRecord<String, String> record = outputTopic.readRecord();
        MessageInfo processedMessage = MessageInfo.deserialize(record.value());
        assertNotEquals(processedMessage.getMessage(), dummyMessage.getMessage());
    }
}
