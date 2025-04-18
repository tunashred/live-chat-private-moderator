package com.github.tunashred.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
// TODO: add logging
public class PreferencesProducer {
    final String preferencesTopic = "streamer-preferences";
    Producer<String, String> producer;

    public PreferencesProducer() throws IOException {
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
            this.producer = new KafkaProducer<>(producerProps);
        }
    }

    public static void main(String[] args) throws IOException {
        PreferencesProducer producer = new PreferencesProducer();
        producer.addPreferences();
        producer.close();
    }

    private static String serializeList(List<String> list) throws JsonProcessingException {
        ObjectMapper writer = new ObjectMapper();
        return writer.writeValueAsString(list);
    }

    public void addPreferences() throws IOException {
        List<String> packsHashes = FileUtil.getFileHash("packs");

        Map<String, List<String>> streamerPreferences = new HashMap<>();
        streamerPreferences.put("satu-mare", Arrays.asList(packsHashes.get(1)));
        streamerPreferences.put("baia-mare", packsHashes);

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
