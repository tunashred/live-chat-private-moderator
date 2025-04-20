package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.privatedtos.ProcessedMessage;
import com.github.tunashred.utils.PreferencesProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaModerator {
    private static final Logger logger = LogManager.getLogger(KafkaModerator.class);
    private static final String sourceTopic = "unsafe_chat";
    private static final String flaggedTopic = "flagged_messages";
    private static final String preferencesTopic = "streamer-preferences";

    private static final PacksData loadedPacks = new PacksData();
    private static PackConsumer packConsumer;
    private static Map<String, List<WordsTrie>> streamerPacks = new HashMap<>();

    public static void main(String[] args) throws RuntimeException, IOException {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Initializing pack consumer.");
        packConsumer = new PackConsumer(loadedPacks, 1000);
        Thread consumerThread = new Thread(packConsumer);
        consumerThread.start();

        logger.info("Initializing KTable and KStream.");

        final Topology topology = createTopology(sourceTopic);

        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(packConsumer::close));

        logger.info("Starting moderator streams application");
        streams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                logger.info("Moderator ready");
            }
        }));
        streams.start();
    }

    // TODO: maybe for future there will be different banned words topics and multiple flagged messages topics
    public static Topology createTopology(String inputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(inputTopic);

        GlobalKTable<String, String> streamerPackPreferences = builder.globalTable(
                preferencesTopic,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(preferencesTopic + "-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled()
        );

        KStream<String, StreamWrapper> wrappedStream = inputStream.mapValues(StreamWrapper::new);

        KStream<String, ProcessedMessage> processedStream = wrappedStream.join(
                streamerPackPreferences,
                (key, value) -> key,
                (wrappedMessage, serializedPreferencesList) -> {
                    logger.trace("Received message to process");
                    String streamerID = wrappedMessage.getKey();
                    UserMessage userMessage;

                    try {
                        userMessage = UserMessage.deserialize(wrappedMessage.getValue());
                    } catch (JsonProcessingException e) {
                        logger.warn("Encountered exception while trying to deserialize record: ", e);
                        return null;
                    }

                    List<WordsTrie> streamerTries = getStreamerTries(streamerID, serializedPreferencesList);

                    return ModeratorPack.censor(streamerTries, userMessage, streamerID);
                }
        );

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

    private static List<WordsTrie> getStreamerTries(String streamerID, String preferences) {
        try {
            List<String> preferencesList = PreferencesProducer.deserializeList(preferences);
            List<WordsTrie> triesList = mapPreferencesToTries(preferencesList);

            streamerPacks.put(streamerID, triesList);

            return triesList;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return streamerPacks.getOrDefault(streamerID, Collections.emptyList());
        }
    }

    private static List<WordsTrie> mapPreferencesToTries(List<String> preferences) {
        return preferences.stream()
                .map(loadedPacks.getPacks()::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
