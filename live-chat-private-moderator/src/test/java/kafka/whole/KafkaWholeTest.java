package kafka.whole;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.GroupChat;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.dtos.User;
import com.github.tunashred.moderator.KafkaModerator;
import com.github.tunashred.moderator.Moderator;
import com.github.tunashred.moderator.WordsTrie;
import kafka.inputs.Dataset;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static kafka.inputs.Dataset.*;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.junit.jupiter.api.Assertions.*;

public class KafkaWholeTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> bannedWordsTopic;

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    static Stream<String> bannedWords() {
        return Arrays.stream(BANNED_WORDS);
    }

    static Stream<Arguments> validClientInfo() {
        int size = Math.min(Math.min(VALID_USERNAMES.length, GROUP_NAMES.length), GOOD_MESSAGES.length);

        return IntStream.range(0, size)
                .mapToObj(i -> Arguments.of(VALID_USERNAMES[i], GROUP_NAMES[i], GOOD_MESSAGES[i]));
    }

    static Stream<Arguments> validClientInfoBadMessages() {
        int size = Math.min(Math.min(VALID_USERNAMES.length, GROUP_NAMES.length), GOOD_MESSAGES.length);

        return IntStream.range(0, size)
                .mapToObj(i -> Arguments.of(VALID_USERNAMES[i], GROUP_NAMES[i], BAD_MESSAGES[i]));
    }

    @BeforeEach
    void setup() {
        final CountDownLatch initialLoadLatch = new CountDownLatch(1);
        Moderator moderator = new Moderator();
        WordsTrie wordsTrie = new WordsTrie();
        final String inputTopicName = "unsafe-messages";
        Topology topology = KafkaModerator.createTopology(inputTopicName, wordsTrie, moderator, initialLoadLatch);

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "kafka-stream-test");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());
        bannedWordsTopic = testDriver.createInputTopic("banned-words", new StringSerializer(), new StringSerializer());
        for (String bannedWord : Dataset.BANNED_WORDS) {
            bannedWordsTopic.pipeInput(bannedWord, bannedWord, 0L);
        }
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @ParameterizedTest
    @MethodSource("bannedWords")
    void removeBannedWord(String bannedWord) {
        bannedWordsTopic.pipeInput(bannedWord, null, 0L);

        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");

        String fetchedWord = bannedWordsStore.get(bannedWord);
        assertNotEquals(fetchedWord, bannedWord);
        assertNull(fetchedWord);
    }

    @ParameterizedTest
    @MethodSource("bannedWords")
    void addBannedWord(String bannedWord) {
        bannedWordsTopic.pipeInput(bannedWord, bannedWord, 0L);

        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");

        String fetchedWord = bannedWordsStore.get(bannedWord);
        assertEquals(fetchedWord, bannedWord);
    }

    @Test
    void removeBatchWords() {
        long timestamp = 0L;
        for (String word : BANNED_WORDS) {
            bannedWordsTopic.pipeInput(word, null, timestamp);
            timestamp += 40L;
        }

        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");

        for (String word : BANNED_WORDS) {
            String fetchedWord = bannedWordsStore.get(word);
            assertNotEquals(fetchedWord, word);
            assertNull(fetchedWord);
        }
    }

    @Test
    void addBatchWords() {
        long timestamp = 0L;
        for (String word : BANNED_WORDS) {
            bannedWordsTopic.pipeInput(word, word, timestamp);
            timestamp += 40L;
        }

        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");

        for (String word : BANNED_WORDS) {
            String fetchedWord = bannedWordsStore.get(word);
            assertEquals(fetchedWord, word);
        }
    }

    private MessageInfo produceMessage(String groupname, String username, String message) throws JsonProcessingException {
        outputTopic = testDriver.createOutputTopic(groupname, new StringDeserializer(), new StringDeserializer());

        MessageInfo messageInfo = new MessageInfo(
                new GroupChat(groupname),
                new User(username),
                message
        );
        String serialized = MessageInfo.serialize(messageInfo);

        // produce
        inputTopic.pipeInput(messageInfo.getGroupChat().getChatID(), serialized);

        return messageInfo;
    }

    @ParameterizedTest
    @MethodSource("validClientInfoBadMessages")
    void sendBadMessages(String groupname, String username, String message) throws JsonProcessingException {
        MessageInfo originalMessage = produceMessage(groupname, username, message);

        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();

        assertEquals(1, records.size());
        assertTrue(outputTopic.isEmpty());

        MessageInfo consumedMessage = MessageInfo.deserialize(records.get(0).value);
        assertNotEquals(originalMessage, consumedMessage);
    }

    @ParameterizedTest
    @MethodSource("validClientInfo")
    void sendGoodMessages(String groupname, String username, String message) throws JsonProcessingException {
        MessageInfo originalMessage = produceMessage(groupname, username, message);

        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();

        assertEquals(1, records.size());
        assertTrue(outputTopic.isEmpty());

        MessageInfo consumedMessage = MessageInfo.deserialize(records.get(0).value);
        assertEquals(originalMessage, consumedMessage);
    }

    @Test
    void multipleClientsOneGroup() throws JsonProcessingException {
        outputTopic = testDriver.createOutputTopic(GROUP_NAMES[0], new StringDeserializer(), new StringDeserializer());
        GroupChat groupChat = new GroupChat(GROUP_NAMES[0]);

        User user_a = new User(VALID_USERNAMES[0]);
        User user_b = new User(VALID_USERNAMES[1]);
        User users[] = {user_a, user_b};

        int size = Math.min(GOOD_MESSAGES.length, BAD_MESSAGES.length);

        Random random = new Random();
        for (int i = 0; i < size; i++) {
            MessageInfo messageInfo = new MessageInfo(groupChat, users[random.nextInt(2)], GOOD_MESSAGES[i]);
            String serialized = MessageInfo.serialize(messageInfo);
            inputTopic.pipeInput(messageInfo.getGroupChat().getChatID(), serialized);

            messageInfo = new MessageInfo(groupChat, users[random.nextInt(2)], BAD_MESSAGES[i]);
            serialized = MessageInfo.serialize(messageInfo);
            inputTopic.pipeInput(messageInfo.getGroupChat().getChatID(), serialized);
        }

        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();

        assertEquals(size * 2, records.size());
    }

    // produce message, a word from the message gets added to the banned words topic, the same message is produced again
    @Test
    void censorWithNewWord() throws JsonProcessingException {
        outputTopic = testDriver.createOutputTopic(GROUP_NAMES[0], new StringDeserializer(), new StringDeserializer());
        GroupChat groupChat = new GroupChat(GROUP_NAMES[0]);
        User user = new User(VALID_USERNAMES[0]);

        String bannedWord = "peste";
        String message = "spor la cafelutsa cu " + bannedWord;
        MessageInfo messageInfo = new MessageInfo(groupChat, user, message);
        String serialized = MessageInfo.serialize(messageInfo);

        inputTopic.pipeInput(groupChat.getChatID(), serialized, 0L);

        TestRecord<String, String> record = outputTopic.readRecord();
        MessageInfo fetchedMessage = MessageInfo.deserialize(record.value());
        assertEquals(messageInfo, fetchedMessage);

        bannedWordsTopic.pipeInput(bannedWord, bannedWord, 1000L);
        inputTopic.pipeInput(groupChat.getChatID(), serialized, 1400L);

        record = outputTopic.readRecord();
        fetchedMessage = MessageInfo.deserialize(record.value());
        assertNotEquals(messageInfo, fetchedMessage);
    }
}
