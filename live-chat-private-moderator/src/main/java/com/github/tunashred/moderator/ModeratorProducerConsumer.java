package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import com.github.tunashred.utils.WordsTrie;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ModeratorProducerConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);
            streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, 10 * 1024 * 1024L);
        } catch (IOException e) {
            e.printStackTrace();
            // maybe add instead some default properties? but then what is the purpose of using an externalized config
            // if not for the fewer lines of code in this file?
            throw new RuntimeException(e.getMessage());
        }

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("unsafe_chat");

        Moderator moderator = new Moderator();
        WordsTrie wordsTrie = new WordsTrie();

        KTable<String, String> bannedWordsTable = builder
                .table(
                        "banned-words",
                        Consumed.with(Serdes.String(), Serdes.String())
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("banned-words-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                                .withCachingDisabled()
                );

        final CountDownLatch initialLoadLatch = new CountDownLatch(1);

        bannedWordsTable.toStream().foreach((key, value) -> {
            System.out.println("Received update for banned word: key - " + key + ", value - " + value);
            if (value == null) {
                wordsTrie.removeWord(key);
            } else {
                wordsTrie.addWord(key);
            }
            moderator.setBannedWords(wordsTrie.getTrie());

            initialLoadLatch.countDown();
        });

        // consume records
        KStream<String, ProcessedMessage> processedStream = inputStream
                .map(((key, value) -> {
                    try {
                        MessageInfo messageInfo = MessageInfo.deserialize(value);
                        ProcessedMessage processedMessage = moderator.censor(messageInfo);
                        System.out.println("\nGroup chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                                "\nmessage.User: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                                "\nOriginal message: " + messageInfo.getMessage() + "\nProcessed message: " + processedMessage.getProcessedMessage());

                        return KeyValue.pair(key, processedMessage);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return null;
                    }
                }));

        // process and send to users
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
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter((_, processedMessage) -> processedMessage != null)
                .to((key, value, recordContext) -> {
                    try {
                        MessageInfo messageInfo = MessageInfo.deserialize(value);
                        return messageInfo.getGroupChat().getChatName();
                    } catch (JsonProcessingException e) {
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
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter((_, processedMessage) -> processedMessage != null)
                .to("flagged_messages");

        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            streams.start();
            System.out.println("Streams application started, waiting for KTable to load...");

            // TODO: rethink this thing
            // it is pretty bad because the moderator can consume and produce before the ktable is loaded
            if (!initialLoadLatch.await(30, TimeUnit.SECONDS)) {
                System.out.println("[WARNING] Timed out while waiting for banned words list initialization!");
                loadStoreManually(streams, wordsTrie, moderator);
            }
            System.out.println("Moderator ready to process messages.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[WARNING] Application interrupted");
        }
    }

    static private void loadStoreManually(KafkaStreams streams, WordsTrie wordsTrie, Moderator moderator) {
        try {
            System.out.println("Manually loading banned words from store...");
            ReadOnlyKeyValueStore<String, String> store =
                    streams.store(StoreQueryParameters.fromNameAndType("banned-words-store", QueryableStoreTypes.keyValueStore()));

            int count = 0;
            try (KeyValueIterator<String, String> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, String> entry = iterator.next();
                    String key = entry.key;
                    String value = entry.value;

                    System.out.println("Manually loading banned word: " + key);

                    if (value == null) {
                        wordsTrie.removeWord(key);
                    } else {
                        wordsTrie.addWord(key);
                    }
                    count++;
                }
            }

            if (wordsTrie.getTrie() == null) {
                System.out.println("[ERROR]: Trie is null after loading the words!");
            } else {
                moderator.setBannedWords(wordsTrie.getTrie());
                System.out.println("All banned words loaded successfully: " + count + " words processed");
            }
        } catch (InvalidStateStoreException e) {
            System.err.println("[ERROR] Failed to access store: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
