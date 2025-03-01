package com.github.tunashred.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

// TODO: add more checks for edge cases
public class BannedWordsManager {
    public static void main(String[] args) throws IOException {
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

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
        final String topicName = input;

        int option = 0;
        while (option < 1 || option > 2) {
            System.out.println("1. Add a word\n2. Remove a word\n");
            input = reader.readLine().trim();
            option = Integer.parseInt(input);
            if (option < 1 || option > 2) {
                System.out.println("Invalid option.\n");
            }
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // TODO: I could maybe have an option to give a list?
        if (option == 1) { // add word logic
            System.out.println("Word to add to " + topicName + " topic: ");
            String word = reader.readLine().trim();

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, word, word);
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("Error while trying to add word: " + e.getMessage());
                } else {
                    System.out.println("Word added!");
                }
            });
        } else if (option == 2) { // remove word logic
            System.out.println("Word to remove to " + topicName + " topic: ");
            input = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, input, null);
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }
}
