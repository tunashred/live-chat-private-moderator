package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.privatedtos.ProcessedMessage;
import com.github.tunashred.utils.PreferencesProducer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE)
public class KafkaModerator {
    static final String sourceTopic = "unsafe_chat";
    static final String flaggedTopic = "flagged_messages";
    static final String preferencesTopic = "streamer-preferences";

    static final PacksData loadedPacks = new PacksData();
    static PackConsumer packConsumer;
    static Map<String, List<WordsTrie>> streamerPacks = new HashMap<>();

    public static void main(String[] args) throws RuntimeException, IOException {
        log.info("Loading streams properties");
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/moderator_streams.properties")) {
            streamsProps.load(propsFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("Initializing pack consumer");
        packConsumer = new PackConsumer(loadedPacks, 1000);
        Thread consumerThread = new Thread(packConsumer);
        consumerThread.start();
        log.info("Pack consumer thread started");

        final Topology topology = createTopology(sourceTopic);

        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(packConsumer::close));

        log.info("Starting moderator streams application");
        streams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                log.info("Moderator ready");
            }
        }));
        streams.start();
    }

    // TODO: maybe for future there will be multiple flagged messages topics?
    public static Topology createTopology(String inputTopic) {
        log.info("Creating topology");
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
                (key, _) -> key,
                (wrappedMessage, serializedPreferencesList) -> {
                    log.trace("Received message to process: " + wrappedMessage);
                    String streamerID = wrappedMessage.getKey();
                    UserMessage userMessage;

                    try {
                        userMessage = UserMessage.deserialize(wrappedMessage.getValue());
                        log.trace("Message deserialized");
                    } catch (JsonProcessingException e) {
                        log.warn("Encountered exception while trying to deserialize record: ", e);
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
                        log.warn("Encountered exception while trying to serialize UserMessage: ", e);
                        return null;
                    }
                })
                .filter((_, value) -> value != null)
                .to(((key, _, _) -> key));

        // if processed message was flagged, then store for later
        processedStream
                .filter((key, processedMessage) -> processedMessage.getIsModerated())
                .map((_, processedMessage) -> {
                    try {
                        String key = processedMessage.getChannelName() + processedMessage.getUserMessage().getUsername();
                        log.trace("Flagged message created: " + processedMessage);
                        return KeyValue.pair(key, ProcessedMessage.serialize(processedMessage));
                    } catch (JsonProcessingException e) {
                        log.warn("Encountered exception while trying to create new record for '" + flaggedTopic + "': ", e);
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
            log.warn("Encountered exception while trying to deserialize list of preferences");
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
