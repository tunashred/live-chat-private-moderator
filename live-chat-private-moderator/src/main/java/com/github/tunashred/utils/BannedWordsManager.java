package com.github.tunashred.utils;

import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// TODO: add more checks for edge cases
public class BannedWordsManager {
    private static KafkaProducer<String, String> producer = null;
    private static KafkaConsumer<String, String> consumer = null;
    @Setter private String topic;

    public BannedWordsManager() {
        // TODO: should I move these to a .properties file?
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        consumer = new KafkaConsumer<>(consumerProps);
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

    public void manageTopic() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Add compacted topic name:");
        String input = reader.readLine().trim();

        // since anyone can create topics if they do not exist, I need to make sure this won't happen accidentally
        // this could be a barbaric way to add valid topics for the operations done by this class
        // though, it might not matter so much since this would be a private tool
        List<String> topics = new ArrayList<>();
        topics.add("banned-words");

        if (!topics.contains(input)) {
            System.out.println("Invalid topic name!");
            return;
        }
        setTopic(input);

        int option = 0;
        while (option < 1 || option > 2) {
            System.out.println("1. Add a word\n2. Remove a word\n");
            input = reader.readLine().trim();
            option = Integer.parseInt(input);
            if (option < 1 || option > 2) {
                System.out.println("Invalid option.\n");
            }
        }

        // TODO: I could maybe have an option to give a list?
        if (option == 1) { // add word logic
            System.out.println("Word to add to " + topic + " topic: ");
            String word = reader.readLine().trim();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, word, word);
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("Error while trying to add word: " + e.getMessage());
                } else {
                    System.out.println("Word added!");
                }
            });
        } else if (option == 2) { // remove word logic
            System.out.println("Word to remove to " + topic + " topic: ");
            input = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, input, null);
            producer.send(record);
        }
        producer.flush();
    }

    public void close() {
        consumer.close();
        producer.close();
    }

    public static void main(String[] args) throws IOException {
        BannedWordsManager manager = new BannedWordsManager();

//        manager.manageTopic();

//        manager.setTopic("banned-words");
//        manager.sendWordsFromFile("packs/banned.txt");

        manager.saveWordsToFile("packs/banned-words-backup.txt");

        manager.close();

        System.out.println("Job's done");
    }
}
