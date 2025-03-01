package com.github.tunashred.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BannedWordsManager {
    public static void main(String[] args) throws IOException {
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Add compacted topic name:");
        String input = reader.readLine().trim();

        // since anyone can create topics if they do not exist, I need to make sure this won't happen accidentally
        if ("banned-words".equals(input)) {
            consumer.subscribe(Collections.singletonList(input));
        } else {
            System.out.println("Invalid topic name. Exiting...");
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

        if (option == 1) { // add word logic
            System.out.println("Word to add to " + topicName + " topic: ");
            input = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, input, input);
            producer.send(record);
            System.out.println("Words added!");
        } else if (option == 2) { // remove word logic
            // first I need to check if the record exists inside the topic
            // actually this means I need the consumer just in case of removing a record

            // I should somehow give it some time to poll the records before I give up either because it takes too much time
            // or the record simply does not exist
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            System.out.println("Word to remove to " + topicName + " topic: ");
            input = reader.readLine();
            // also, I could maybe have an option to give a list?
            for (var consumerRecord : consumerRecords) {
                if (consumerRecord.key().equals(input)) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, input, null);
                    producer.send(record);
                    System.out.println("Word removed!");
                    break;
                }
            }
        }
        producer.close();
    }
}
