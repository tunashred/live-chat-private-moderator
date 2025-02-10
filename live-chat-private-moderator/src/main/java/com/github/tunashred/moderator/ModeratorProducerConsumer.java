package com.github.tunashred.moderator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ModeratorProducerConsumer {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerProps.put(GROUP_ID_CONFIG, "moderator-consumer-group");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("acks", "all");

        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("unsafe_chat"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        Moderator moderator = new Moderator("packs/banned.txt");
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    MessageInfo messageInfo = objectMapper.readValue(record.value(), MessageInfo.class);

                    ProcessedMessage processedMessage = moderator.censor(messageInfo);
                    System.out.println("\nGroup chat: " + messageInfo.getGroupChat().getChatName() + "/" + messageInfo.getGroupChat().getChatID() +
                            "\nmessage.User: " + messageInfo.getUser().getName() + "/" + messageInfo.getUser().getUserID() +
                            "\nOriginal message: " + messageInfo.getMessage() + "\nProcessed message: " + processedMessage.getProcessedMessage());

                    if (processedMessage.isCensored()) {
                        producer.send(new ProducerRecord<>("flagged_messages", record.key(), objectMapper.writeValueAsString(processedMessage)));
                    }
                    String toDeliverMessageInfo = objectMapper.writeValueAsString(new MessageInfo(processedMessage.getMessageInfo().getGroupChat(), processedMessage.getMessageInfo().getUser(), processedMessage.getProcessedMessage()));
                    producer.send(new ProducerRecord<>("safe_chat", record.key(), toDeliverMessageInfo));
                    consumer.commitSync();
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
            producer.close();
        }
    }
}