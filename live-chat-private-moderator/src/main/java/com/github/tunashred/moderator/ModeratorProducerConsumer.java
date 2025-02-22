package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

        Moderator moderator = new Moderator("packs/banned.txt");

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
                .to("safe_chat");

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