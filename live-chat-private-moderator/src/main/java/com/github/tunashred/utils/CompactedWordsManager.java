package com.github.tunashred.utils;

import com.github.tunashred.privatedtos.ProcessedMessage;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.io.*;
import java.util.Properties;

public class CompactedWordsManager {
    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/words_manager_streams.properties")) {
            streamsProps.load(propsFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream;

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Add compacted topic name:");
        String input = reader.readLine().trim();

        // since anyone can create topics if they do not exist, I need to make sure this won't happen accidentally
        if ("banned-words".equals(input)) {
            inputStream = builder.stream("banned-words");
        } else {
            System.out.println("Invalid topic name. Exiting...");
            return;
        }

        System.out.println("1. Add a word\n2. Remove a word");
        input = reader.readLine().trim();
        int option = Integer.parseInt(input);
        if (option == 1) {
            // add word logic
        } else if (option == 2) {
            // remove word logic
        } else {
            System.out.println("Invalid topic operation option.");
            return;
        }



//        KStream<String, ProcessedMessage> processedStream = inputStream
    }
}
