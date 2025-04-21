package com.github.tunashred.moderator;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.ahocorasick.trie.Trie;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class WordsTrie {
    Set<String> words = new HashSet<>();
    Trie trie;
    String sourceFile;

    public WordsTrie(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public void addWord(String word) {
        if (word == null) {
            return;
        }

        if (words.add(word)) {
            rebuild();
        }
    }

    public void removeWord(String word) {
        if (word == null) {
            return;
        }

        if (words.remove(word)) {
            rebuild();
        }
    }

    public void addBatch(List<String> wordsToAdd) {
        boolean tainted = false;

        for (String word : wordsToAdd) {
            if (words.add(word)) {
                tainted = true;
            }
        }

        if (tainted) {
            rebuild();
        }
    }

    public void updateBatch(List<String> wordsToAdd, List<String> wordsToRemove) {
        boolean tainted = false;

        for (String word : wordsToRemove) {
            if (words.remove(word)) {
                tainted = true;
            }
        }

        for (String word : wordsToAdd) {
            if (words.add(word)) {
                tainted = true;
            }
        }

        if (tainted) {
            rebuild();
        }
    }

    private void rebuild() {
        Trie.TrieBuilder builder = Trie.builder().ignoreCase();
        for (String word : words) {
            builder.addKeyword(word.trim().toLowerCase());
        }
        this.trie = builder.build();
    }
}
