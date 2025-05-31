package com.github.tunashred.packs;

import com.github.tunashred.moderator.WordsTrie;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PacksData {
    // TODO: switch to a normal hashmap?
    Map<String, WordsTrie> packs = new ConcurrentHashMap<>();

    public void addWord(String topic, String word) {
        packs.computeIfAbsent(topic, w -> new WordsTrie())
                .addWord(word);
    }

    public void removeWord(String topic, String word) {
        WordsTrie trie = packs.get(topic);
        if (trie != null) {
            trie.removeWord(word);
            if (trie.getWords().isEmpty()) {
                packs.remove(topic);
            }
        }
    }
}
