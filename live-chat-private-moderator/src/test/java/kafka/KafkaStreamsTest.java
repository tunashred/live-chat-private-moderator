//package kafka;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.github.tunashred.dtos.UserMessage;
//import com.github.tunashred.moderator.KafkaModerator;
//import com.github.tunashred.moderator.Moderator;
//import com.github.tunashred.moderator.WordsTrie;
//import kafka.inputs.Dataset;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.streams.*;
//import org.apache.kafka.streams.state.KeyValueStore;
//import org.apache.kafka.streams.test.TestRecord;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//import java.util.Random;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//import static kafka.inputs.Dataset.*;
//import static org.apache.kafka.streams.StreamsConfig.*;
//import static org.junit.jupiter.api.Assertions.*;
//
//public class KafkaStreamsTest {
//    private TopologyTestDriver testDriver;
//    private TestInputTopic<String, String> bannedWordsTopic;
//
//    private TestInputTopic<String, String> inputTopic;
//    private TestOutputTopic<String, String> outputTopic;
//
//    static Stream<String> bannedWords() {
//        return Arrays.stream(BANNED_WORDS);
//    }
//
//    static Stream<Arguments> validClientInfo() {
//        int size = Math.min(Math.min(VALID_USERNAMES.length, GROUP_NAMES.length), GOOD_MESSAGES.length);
//
//        return IntStream.range(0, size)
//                .mapToObj(i -> Arguments.of(GROUP_NAMES[i], VALID_USERNAMES[i], GOOD_MESSAGES[i]));
//    }
//
//    static Stream<Arguments> validClientInfoBadMessages() {
//        int size = Math.min(Math.min(VALID_USERNAMES.length, GROUP_NAMES.length), BAD_MESSAGES.length);
//
//        return IntStream.range(0, size)
//                .mapToObj(i -> Arguments.of(GROUP_NAMES[i], VALID_USERNAMES[i], BAD_MESSAGES[i]));
//    }
//
//    @BeforeEach
//    void setup() {
//        Moderator moderator = new Moderator();
//        WordsTrie wordsTrie = new WordsTrie();
//        final String inputTopicName = "unsafe-messages";
//        Topology topology = KafkaModerator.createTopology(inputTopicName, wordsTrie, moderator);
//
//        Properties props = new Properties();
//        props.put(APPLICATION_ID_CONFIG, "kafka-stream-test");
//        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
//        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//
//        testDriver = new TopologyTestDriver(topology, props);
//
//        inputTopic = testDriver.createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());
//        bannedWordsTopic = testDriver.createInputTopic("banned-words", new StringSerializer(), new StringSerializer());
//        for (String bannedWord : Dataset.BANNED_WORDS) {
//            bannedWordsTopic.pipeInput(bannedWord, bannedWord, 0L);
//        }
//    }
//
//    @AfterEach
//    void tearDown() {
//        testDriver.close();
//    }
//
//    @ParameterizedTest
//    @MethodSource("bannedWords")
//    void removeBannedWord(String bannedWord) {
//        bannedWordsTopic.pipeInput(bannedWord, null, 0L);
//
//        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");
//
//        String fetchedWord = bannedWordsStore.get(bannedWord);
//        assertNotEquals(fetchedWord, bannedWord);
//        assertNull(fetchedWord);
//    }
//
//    @ParameterizedTest
//    @MethodSource("bannedWords")
//    void addBannedWord(String bannedWord) {
//        bannedWordsTopic.pipeInput(bannedWord, bannedWord, 0L);
//
//        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");
//
//        String fetchedWord = bannedWordsStore.get(bannedWord);
//        assertEquals(fetchedWord, bannedWord);
//    }
//
//    @Test
//    void removeBatchWords() {
//        long timestamp = 0L;
//        for (String word : BANNED_WORDS) {
//            bannedWordsTopic.pipeInput(word, null, timestamp);
//            timestamp += 40L;
//        }
//
//        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");
//
//        for (String word : BANNED_WORDS) {
//            String fetchedWord = bannedWordsStore.get(word);
//            assertNotEquals(fetchedWord, word);
//            assertNull(fetchedWord);
//        }
//    }
//
//    @Test
//    void addBatchWords() {
//        long timestamp = 0L;
//        for (String word : BANNED_WORDS) {
//            bannedWordsTopic.pipeInput(word, word, timestamp);
//            timestamp += 40L;
//        }
//
//        KeyValueStore<String, String> bannedWordsStore = testDriver.getKeyValueStore("banned-words-store");
//
//        for (String word : BANNED_WORDS) {
//            String fetchedWord = bannedWordsStore.get(word);
//            assertEquals(fetchedWord, word);
//        }
//    }
//
//    private UserMessage produceMessage(String groupname, String username, String message) throws JsonProcessingException {
//        outputTopic = testDriver.createOutputTopic(groupname, new StringDeserializer(), new StringDeserializer());
//
//        UserMessage userMessage = new UserMessage(username, message);
//        String serialized = UserMessage.serialize(userMessage);
//
//        // produce
//        inputTopic.pipeInput(groupname, serialized);
//
//        return userMessage;
//    }
//
//    @ParameterizedTest
//    @MethodSource("validClientInfoBadMessages")
//    void sendBadMessages(String groupname, String username, String message) throws JsonProcessingException {
//        UserMessage originalMessage = produceMessage(groupname, username, message);
//
//        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();
//
//        assertEquals(1, records.size());
//        assertTrue(outputTopic.isEmpty());
//
//        UserMessage consumedMessage = UserMessage.deserialize(records.get(0).value);
//        assertNotEquals(originalMessage, consumedMessage);
//    }
//
//    @ParameterizedTest
//    @MethodSource("validClientInfo")
//    void sendGoodMessages(String groupname, String username, String message) throws JsonProcessingException {
//        UserMessage originalMessage = produceMessage(groupname, username, message);
//
//        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();
//
//        assertEquals(1, records.size());
//        assertTrue(outputTopic.isEmpty());
//
//        UserMessage consumedMessage = UserMessage.deserialize(records.get(0).value);
//        System.out.println(originalMessage.getUsername() + ": " + originalMessage.getMessage());
//        System.out.println(consumedMessage.getUsername() + ": " + consumedMessage.getMessage());
//        assertEquals(originalMessage, consumedMessage);
//    }
//
//    @Test
//    void multipleClientsOneGroup() throws JsonProcessingException {
//        outputTopic = testDriver.createOutputTopic(GROUP_NAMES[0], new StringDeserializer(), new StringDeserializer());
//        String groupChat = GROUP_NAMES[0];
//
//        String user_a = VALID_USERNAMES[0];
//        String user_b = VALID_USERNAMES[1];
//        String users[] = {user_a, user_b};
//
//        int size = Math.min(GOOD_MESSAGES.length, BAD_MESSAGES.length);
//
//        Random random = new Random();
//        for (int i = 0; i < size; i++) {
//            UserMessage userMessage = new UserMessage(users[random.nextInt(2)], GOOD_MESSAGES[i]);
//            String serialized = UserMessage.serialize(userMessage);
//            inputTopic.pipeInput(groupChat, serialized);
//
//            userMessage = new UserMessage(users[random.nextInt(2)], BAD_MESSAGES[i]);
//            serialized = UserMessage.serialize(userMessage);
//            inputTopic.pipeInput(groupChat, serialized);
//        }
//
//        List<KeyValue<String, String>> records = outputTopic.readKeyValuesToList();
//
//        assertEquals(size * 2, records.size());
//    }
//
//    // produce message, a word from the message gets added to the banned words topic, the same message is produced again
//    @Test
//    void censorWithNewWord() throws JsonProcessingException {
//        outputTopic = testDriver.createOutputTopic(GROUP_NAMES[0], new StringDeserializer(), new StringDeserializer());
//        String groupChat = GROUP_NAMES[0];
//        String user = VALID_USERNAMES[0];
//
//        String bannedWord = "peste";
//        String message = "spor la cafelutsa cu " + bannedWord;
//        UserMessage userMessage = new UserMessage(user, message);
//        String serialized = UserMessage.serialize(userMessage);
//
//        inputTopic.pipeInput(groupChat, serialized, 0L);
//
//        TestRecord<String, String> record = outputTopic.readRecord();
//        UserMessage fetchedMessage = UserMessage.deserialize(record.value());
//        assertEquals(userMessage, fetchedMessage);
//
//        bannedWordsTopic.pipeInput(bannedWord, bannedWord, 1000L);
//        inputTopic.pipeInput(groupChat, serialized, 1400L);
//
//        record = outputTopic.readRecord();
//        fetchedMessage = UserMessage.deserialize(record.value());
//        assertNotEquals(userMessage, fetchedMessage);
//    }
//}
