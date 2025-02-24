package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import com.github.tunashred.utils.WordsTrie;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ModeratorProducerConsumer {
    public static void main(String[] args) {
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
        List<String> wordsToAdd = new ArrayList<>();
        List<String> wordsToRemove = new ArrayList<>();

        KTable<String, String> bannedWordsTable = builder
                .table("banned-words", Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("banned-words-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        bannedWordsTable.toStream().foreach(((key, value) -> {
            System.out.println(value);
            if (value == null) {
                wordsToRemove.add(key);
            } else {
                wordsToAdd.add(key);
            }
        }));

        wordsTrie.updateBatch(wordsToAdd, wordsToRemove);
        moderator.setBannedWords(wordsTrie.getTrie());

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
        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}