package com.github.tunashred.utils;

import com.github.tunashred.moderator.WordsTrie;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

// warning: this class became obsolete after various design decisions and shifts in perspective
public class FileUtil {
    public static void main(String[] args) throws IOException {
        getFileNames("packs");
        loadPacks("packs");
    }

    static public Map<String, WordsTrie> loadPacks(String dirPath) throws IOException {
        Map<String, WordsTrie> packs = new HashMap<>();

        Files.list(Paths.get(dirPath))
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    String fileName = file.getFileName().toString();
                    String hash = String.valueOf(file.hashCode());

                    WordsTrie trie = new WordsTrie(fileName);
                    List<String> words = new ArrayList<>();

                    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            words.add(line);
                            System.out.println(line);
                        }
                        trie.addBatch(words);
                        packs.put(hash, trie);
                        System.out.println("Loaded file " + fileName + " with hash " + hash);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        return packs;
    }

    public static Boolean fileExists(Path path) {
        if (!Files.isRegularFile(path) || !Files.exists(path)) {
            System.out.println("File does not exist");
            return false;
        }
        return true;
    }

    static public List<String> getFileNames(String dirPath) throws IOException {
        List<String> files = Files.list(Paths.get(dirPath))
                .filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString())
                .collect(Collectors.toList());

        return files;
    }

    static public List<String> getFileHash(String dirPath) throws IOException {
        List<String> files = Files.list(Paths.get(dirPath))
                .filter(Files::isRegularFile)
                .map(path -> String.valueOf(path.hashCode()))
                .collect(Collectors.toList());

        return files;
    }

    // TODO: revisit the logic here
    // TODO 2: seems like this does not work properly
    static public String getFileHashByName(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        if (!fileExists(path)) {
            throw new FileNotFoundException();
        }
        byte[] fileBytes = Files.readAllBytes(path);
        return String.valueOf(Arrays.hashCode(fileBytes));
    }

}
