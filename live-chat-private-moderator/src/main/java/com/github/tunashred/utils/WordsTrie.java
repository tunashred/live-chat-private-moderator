package com.github.tunashred.utils;

import lombok.Getter;
import org.ahocorasick.trie.Trie;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WordsTrie {
    private final Set<String> words = new HashSet<>();
    @Getter
    private Trie trie;

    public WordsTrie() {
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
    }
}
