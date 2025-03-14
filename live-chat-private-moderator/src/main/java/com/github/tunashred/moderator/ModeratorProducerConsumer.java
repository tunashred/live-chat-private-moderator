package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ModeratorProducerConsumer {
    private static final Logger logger = LogManager.getLogger(ModeratorProducerConsumer.class);
    private static final String sourceTopic = "unsafe_chat";
    private static final String bannedWordsTopic = "banned-words";
    private static final String flaggedTopic = "flagged_messages";

    public static void main(String[] args) {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);
            streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 10 * 1024 * 1024L);
        } catch (IOException e) {
            logger.error("Failed to load kafka streams properties file: ", e);
            // TODO: should I keep this?
            // maybe add instead some default properties? but then what is the purpose of using an externalized config
            // if not for the fewer lines of code in this file?
            throw new RuntimeException();
        }

        logger.info("Initializing KTable and KStream.");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(sourceTopic);

        Moderator moderator = new Moderator();
        WordsTrie wordsTrie = new WordsTrie();

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
        final CountDownLatch initialLoadLatch = new CountDownLatch(1);

        bannedWordsTable.toStream().foreach((key, value) -> {
            logger.info("Received update for banned word: key - " + key + ", value - " + value);
            if (value == null) {
                wordsTrie.removeWord(key);
            } else {
                wordsTrie.addWord(key);
            }
            moderator.setBannedWords(wordsTrie.getTrie());

            initialLoadLatch.countDown();
        });

        // consume and process
        KStream<String, ProcessedMessage> processedStream = inputStream
                .map(((key, value) -> {
                    try {
                        MessageInfo messageInfo = MessageInfo.deserialize(value);
                        ProcessedMessage processedMessage = moderator.censor(messageInfo);
                        logger.info("\nGroup chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                                "\nmessage.User: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                                "\nOriginal message: " + messageInfo.getMessage() + "\nProcessed message: " + processedMessage.getProcessedMessage());

                        return KeyValue.pair(key, processedMessage);
                    } catch (JsonProcessingException e) {
                        // TODO: revisit this print
                        logger.warn("Encountered exception while trying to deserialize record: ", e);
                        return null;
                    }
                }));

        // produce
        processedStream
                .map((key, processedMessage) -> {
                    try {
                        return KeyValue.pair(key, MessageInfo.serialize(
                                        new MessageInfo(processedMessage.getMessageInfo().getGroupChat(),
                                                processedMessage.getMessageInfo().getUser(),
                                                processedMessage.getProcessedMessage())
                                )
                        );
                    } catch (JsonProcessingException e) {
                        logger.warn("Encountered exception while trying to create new record: ", e);
                        return null;
                    }
                })
                .filter((_, processedMessage) -> processedMessage != null)
                .to((key, value, recordContext) -> {
                    try {
                        MessageInfo messageInfo = MessageInfo.deserialize(value);
                        return messageInfo.getGroupChat().getChatName();
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to fetch topic name for sending record: ", e);
                        // TODO: should I keep this?
                        throw new RuntimeException(e);
                    }
                });

        // if processed message was flagged, then store for later
        processedStream
                .filter((key, processedMessage) -> processedMessage.isCensored())
                .map((key, processedMessage) -> {
                    try {
                        return KeyValue.pair(key, ProcessedMessage.serialize(processedMessage));
                    } catch (JsonProcessingException e) {
                        logger.warn("Encountered exception while trying to create new record for '" + flaggedTopic + "': ", e);
                        return null;
                    }
                })
                .filter((_, processedMessage) -> processedMessage != null)
                .to(flaggedTopic);

        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            logger.info("Starting moderator streams application");
            streams.start();

            int timeout = 30;
            logger.info("Waiting " + timeout + " seconds for KTable to fetch record from topic '" + bannedWordsTopic + "'");
            // TODO: rethink this thing
            // it is pretty bad because the moderator can consume and produce before the ktable is loaded
            if (!initialLoadLatch.await(timeout, TimeUnit.SECONDS)) {
                logger.warn("Timed out while waiting for banned words list to be loaded");
                loadStoreManually(streams, wordsTrie, moderator);
            }
            logger.info("Moderator ready to process messages");
        } catch (InterruptedException e) {
            logger.warn("Application interrupted: ", e);
            Thread.currentThread().interrupt();
        }
    }

    static private void loadStoreManually(KafkaStreams streams, WordsTrie wordsTrie, Moderator moderator) {
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
                logger.error("Trie is empty after manual load");
            } else {
                moderator.setBannedWords(wordsTrie.getTrie());
                logger.info("All banned words loaded successfully: " + count + " words processed");
            }
        } catch (InvalidStateStoreException e) {
            logger.error("Failed to access store '" + storeName + "': ", e);
        }
    }
}
