package com.github.tunashred.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class BannedWordsProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public BannedWordsProducer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        BannedWordsProducer producer = new BannedWordsProducer("banned-words");
        producer.sendWordsFromFile("packs/banned.txt");
        producer.close();

        System.out.println("Words loaded.");
    }

    public void sendWordsFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String word = line.trim();
                if (!word.isEmpty()) {
                    producer.send(new ProducerRecord<>(topic, word, word));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }
}
