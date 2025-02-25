package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import com.github.tunashred.utils.WordsTrie;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
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

public class ModeratorProducerConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);
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
                .table("banned-words", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("banned-words-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        bannedWordsTable.toStream().foreach(((key, value) -> {
            System.out.println(value);
            if (value == null) {
                wordsTrie.removeWord(key);
            } else {
                wordsTrie.addWord(key);
            }
            moderator.setBannedWords(wordsTrie.getTrie());
        }));

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
        streams.start();

        // so called 'waiting' for ktable to be loaded
        // but does not do anything and I end up with the banned-words-store being empty
        ReadOnlyKeyValueStore<String, String> bannedWordsStore = null;
        while (bannedWordsStore == null) {
            try {
                bannedWordsStore = streams.store(StoreQueryParameters.fromNameAndType("banned-words-store", QueryableStoreTypes.keyValueStore()));
                System.out.println("Store is now available");
            } catch (InvalidStateStoreException invalidStateStoreException) {
                try {
                    System.out.println("Store not ready, retrying...");
                    Thread.sleep(500);
                } catch (InterruptedException interruptedException) {
                    // maybe should be doing something else too ?
                    interruptedException.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }

        while (true) {
            try (KeyValueIterator<String, String> iterator = bannedWordsStore.all()) {
                if (iterator.hasNext()) break; // Exit when at least one record is found
            }
            System.out.println("Waiting for banned words store to be populated...");
            Thread.sleep(500);
        }

        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}