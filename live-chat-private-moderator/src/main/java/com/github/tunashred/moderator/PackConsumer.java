package com.github.tunashred.moderator;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class PackConsumer implements Runnable {
    private static final Logger logger = LogManager.getLogger(PackConsumer.class);
    private static KafkaConsumer<String, String> consumer = null;
    private final PacksData packsData;
    private final long sleepMillis;
    private volatile boolean running = true;

    public PackConsumer(PacksData packsData, long sleepMillis) throws IOException {
        logger.info("Loading consumer properties");
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumerProps.put(GROUP_ID_CONFIG, "consumer-packs-2"); // TODO: revise this
            this.consumer = new KafkaConsumer<>(consumerProps);
        }
        this.packsData = packsData;
        this.sleepMillis = sleepMillis;
        logger.info("Consumer created");
    }

    private static void jumpToBeginning() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!consumer.assignment().isEmpty()) {
                consumer.seekToBeginning(consumer.assignment());
                break;
            }
        }
    }

    @Override
    public void run() {
        logger.info("Consumer started");
        Pattern pattern = Pattern.compile("^pack-.*");
        consumer.subscribe(pattern);
        jumpToBeginning();

        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    String topic = record.topic();
                    logger.trace("Record consumed from pack " + topic);
                    if (record.value() != null) {
                        packsData.addWord(topic, record.key());
                    } else {
                        packsData.removeWord(topic, record.key());
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
        logger.info("Closing consumer");
        consumer.close();
    }

    public void close() {
        running = false;
        consumer.wakeup();
    }
}
