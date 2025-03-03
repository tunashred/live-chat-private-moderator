package com.github.tunashred.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class BannedWordsFileLoader {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public BannedWordsFileLoader(String topic) {
        this.topic = topic;
        // TODO: should I move these to a .properties file?
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProps.put(GROUP_ID_CONFIG, "words-manager");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ACKS_CONFIG, "all");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public static void main(String[] args) {
        BannedWordsFileLoader loader = new BannedWordsFileLoader("banned-words");
//        loader.sendWordsFromFile("packs/banned.txt");

        loader.saveWordsToFile("packs/banned-words-backup.txt");

        loader.close();
        System.out.println("Job's done");
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
        } finally {
            System.out.println("Words loaded.");
        }
    }

    // TODO: for future, this code must be more robust and cleaner
    public void saveWordsToFile(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "utf-8"))) {
            List<String> wordsList = new ArrayList<>();
            consumer.subscribe(Collections.singletonList("banned-words"));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(6000));
                if (consumerRecords.isEmpty()) {
                    System.out.println("Done consuming records.");
                    break;
                }
                for (var record : consumerRecords) {
                    System.out.println("Consumed record key: " + record.key());
                    wordsList.add(record.key());
                }
            }
            consumer.close();

            for (String word : wordsList) {
                writer.write(word);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}
