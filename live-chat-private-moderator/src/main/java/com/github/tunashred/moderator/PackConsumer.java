package com.github.tunashred.moderator;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@FieldDefaults(level = AccessLevel.PRIVATE)
@Log4j2
public class PackConsumer implements Runnable {
    static KafkaConsumer<String, Boolean> consumer = null;
    static volatile boolean running = true;
    final PacksData packsData;
    final long sleepMillis;

    public PackConsumer(PacksData packsData, long sleepMillis) throws IOException {
        log.info("Loading consumer properties");
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-packs-1"); // TODO: revise this
            consumer = new KafkaConsumer<>(consumerProps);
        }
        this.packsData = packsData;
        this.sleepMillis = sleepMillis;
        log.info("Consumer created");
    }

    private static void jumpToBeginning() {
        log.info("Seeking to beginning of partitions");
        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(100));
            log.trace("Polling for partition allocation");
        }
        consumer.seekToBeginning(consumer.assignment());
        log.trace("Offsets set to beginning");
    }

    @Override
    public void run() {
        log.info("Consumer started");
        Pattern pattern = Pattern.compile("^pack-.*");
        consumer.subscribe(pattern);
        jumpToBeginning();
        log.info("Subscribed to topics: {}",
                consumer.assignment().stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet())
        );

        while (running) {
            try {
                log.trace("Polling for packs updates");
                ConsumerRecords<String, Boolean> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    log.trace("Record to be processed: " + record);
                    String topic = record.topic();
                    log.trace("Record consumed from pack " + topic);
                    if (record.value()) {
                        packsData.addWord(topic, record.key());
                        log.trace("New word '" + record.key() + "'" + " from pack topic '" + topic + "'");
                    } else {
                        packsData.removeWord(topic, record.key());
                        log.trace("Removed word '" + record.key() + "'" + " from pack topic '" + topic + "'");
                    }
                }
                consumer.commitSync();
                Thread.sleep(sleepMillis);
            } catch (WakeupException e) {
                if (!running) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Closing consumer");
        consumer.close();
    }

    public void close() {
        running = false;
        consumer.wakeup();
    }
}
