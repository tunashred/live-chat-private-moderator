package com.github.tunashred.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
// TODO: add logging
public class PreferencesProducer {
    final String preferencesTopic = "streamer-preferences";
    private List<String> packs;
    Producer<String, String> producer;
    KafkaConsumer<String, String> consumer;

    public PreferencesProducer() throws IOException {
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
            this.producer = new KafkaProducer<>(producerProps);
        }

        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-preferences-producer-1"); // TODO: revise this
            this.consumer = new KafkaConsumer<>(consumerProps);
        }

        this.packs = new ArrayList<>();
    }

    public static void main(String[] args) throws IOException {
        PreferencesProducer producer = new PreferencesProducer();
        producer.addPreferences();
        producer.close();
    }

    public static String serializeList(List<String> list) throws JsonProcessingException {
        ObjectMapper writer = new ObjectMapper();
        return writer.writeValueAsString(list);
    }

    public static List<String> deserializeList(String preferences) throws JsonProcessingException {
        ObjectMapper reader = new ObjectMapper();
        return reader.readValue(preferences, new TypeReference<ArrayList<String>>() {
        });
    }

    public void loadPackTopics() {
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        for (String topic : topicMap.keySet()) {
            if (topic.startsWith("pack-")) {
                packs.add(topic);
            }
        }
        consumer.close();
    }

    public void addPreferences() throws IOException {
        loadPackTopics();
        Map<String, List<String>> streamerPreferences = new HashMap<>();
        streamerPreferences.put("satu-mare", Collections.singletonList(packs.get(1)));
        streamerPreferences.put("baia-mare", packs);

        for (Map.Entry<String, List<String>> preference : streamerPreferences.entrySet()) {
            try {
                producer.send(new ProducerRecord<>(preferencesTopic, preference.getKey(), serializeList(preference.getValue())));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // is it recommended to flush before closing?
    public void close() {
        this.producer.flush();
        this.producer.close();
    }
}
