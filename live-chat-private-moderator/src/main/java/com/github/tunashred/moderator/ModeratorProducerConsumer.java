package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class ModeratorProducerConsumer {
    public static void main(String[] args) {
        Properties streamsProps = new Properties();
        streamsProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        streamsProps.put(APPLICATION_ID_CONFIG, "moderator-application");
        streamsProps.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("unsafe_chat");

        Moderator moderator = new Moderator("packs/banned.txt");

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
                        throw new RuntimeException(e);
                    }
                }));

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
                        throw new RuntimeException(e);
                    }
                }).to("safe_chat");

        processedStream
                .filter((key, processedMessage) -> processedMessage.isCensored())
                .map((key, processedMessage) -> {
                    try {
                        return KeyValue.pair(key, ProcessedMessage.serialize(processedMessage));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .to("flagged_messages");

        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsProps);
        streams.start();
        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}