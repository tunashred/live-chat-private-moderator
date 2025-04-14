package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class KafkaModerator {
    private static final Logger logger = LogManager.getLogger(KafkaModerator.class);
    private static final String sourceTopic = "unsafe_chat";
    private static final String bannedWordsTopic = "banned-words";
    private static final String flaggedTopic = "flagged_messages";

    public static void main(String[] args) throws RuntimeException {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        logger.info("Initializing KTable and KStream.");

        Moderator moderator = new Moderator();
        WordsTrie wordsTrie = new WordsTrie();

        final Topology topology = createTopology(sourceTopic, wordsTrie, moderator);

        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        logger.info("Starting moderator streams application");
        loadStoreManually(streams, wordsTrie, moderator);
        streams.start();

    }

    // TODO: maybe for future there will be different banned words topics and multiple flagged messages topics
    public static Topology createTopology(String inputTopic, WordsTrie wordsTrie, Moderator moderator) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(inputTopic);

        KTable<String, String> bannedWordsTable = builder
                .table(
                        bannedWordsTopic,
                        Consumed.with(Serdes.String(), Serdes.String())
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("banned-words-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                                .withCachingDisabled()
                );

        bannedWordsTable.toStream().foreach((key, value) -> {
            logger.info("Received update for banned word: key - " + key + ", value - " + value);
            if (value == null) {
                wordsTrie.removeWord(key);
            } else {
                wordsTrie.addWord(key);
            }
            moderator.setBannedWords(wordsTrie.getTrie());
        });

        // consume and process
        KStream<String, ProcessedMessage> processedStream = inputStream
                .map(((key, value) -> {
                    try {
                        UserMessage userMessage = UserMessage.deserialize(value);
                        ProcessedMessage processedMessage = moderator.censor(userMessage, key);
                        logger.trace(() -> new MapMessage<>(Map.of(
                                "GroupChat", key,
                                "User", userMessage.getUsername(),
                                "Original message", processedMessage.getOriginalMessage(),
                                "Processed message", processedMessage.getUserMessage().getMessage()
                        )));

                        return KeyValue.pair(key, processedMessage);
                    } catch (JsonProcessingException e) {
                        logger.warn("Encountered exception while trying to deserialize record: ", e);
                        return null;
                    }
                }));

        processedStream
                .map((key, processedMessage) -> {
                    String topicName = processedMessage.getChannelName();
                    try {
                        String serialized = UserMessage.serialize(processedMessage.getUserMessage());
                        return KeyValue.pair(topicName, serialized);
                    } catch (JsonProcessingException e) {
                        // TODO: add logger
                        return null;
                    }
                })
                .filter((_, value) -> value != null)
                .to(((key, value, ctx) -> key));

        // if processed message was flagged, then store for later
        processedStream
                .filter((key, processedMessage) -> processedMessage.getIsModerated())
                .map((_, processedMessage) -> {
                    try {
                        String key = processedMessage.getChannelName() + processedMessage.getUserMessage().getUsername();
                        return KeyValue.pair(key, ProcessedMessage.serialize(processedMessage));
                    } catch (JsonProcessingException e) {
                        logger.warn("Encountered exception while trying to create new record for '" + flaggedTopic + "': ", e);
                        return null;
                    }
                })
                .filter((_, processedMessage) -> processedMessage != null)
                .to(flaggedTopic);

        return builder.build();
    }

    static private void loadStoreManually(KafkaStreams streams, WordsTrie wordsTrie, Moderator moderator) {
        streams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                logger.info("Moderator is now running");

                loadStore(streams, wordsTrie, moderator);
            }
        }));
    }

    static private void loadStore(KafkaStreams streams, WordsTrie wordsTrie, Moderator moderator) {
        final String storeName = bannedWordsTopic + "-store";
        try {
            logger.info("Trying to load manually the words into from store");
            ReadOnlyKeyValueStore<String, String> store =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

            int count = 0;
            try (KeyValueIterator<String, String> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, String> entry = iterator.next();
                    String key = entry.key;
                    String value = entry.value;

                    logger.info("Manually added word: " + key);

                    if (value == null) {
                        wordsTrie.removeWord(key);
                    } else {
                        wordsTrie.addWord(key);
                    }
                    count++;
                }
            }

            if (wordsTrie.getTrie() == null) {
                logger.error("KTable store is empty");
            } else {
                moderator.setBannedWords(wordsTrie.getTrie());
                logger.info("All banned words loaded successfully: " + count + " words processed");
            }
        } catch (InvalidStateStoreException e) {
            logger.error("Failed to access store '" + storeName + "': ", e);
        }
    }
}
