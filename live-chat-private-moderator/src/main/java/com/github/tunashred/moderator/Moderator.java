package com.github.tunashred.moderator;

import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

// warning: this class became obsolete after various design decisions and shifts in perspective
public class Moderator {
    private Trie bannedWords;

    public Moderator(String file_path) {
        Trie.TrieBuilder trieBuilder = Trie.builder().ignoreCase();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file_path));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                trieBuilder.addKeyword(line.trim().toLowerCase());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.bannedWords = trieBuilder.build();
    }

    public Moderator() {
        // this means messages coming before ktable is loaded, are NOT going to be censored
        this.bannedWords = Trie.builder().build();
    }

    public ProcessedMessage censor(UserMessage userMessage, String channel) {
        String message = userMessage.getMessage();
        StringBuilder censoredMessage = new StringBuilder(message);
        boolean isCensored = false;

        for (Emit emit : bannedWords.parseText(message)) {
            int start = emit.getStart(), end = emit.getEnd();
            String replacement = "*".repeat(emit.getKeyword().length());
            censoredMessage.replace(start, end + 1, replacement);
            isCensored = true;
        }

        UserMessage moderatedUserMessage = new UserMessage(userMessage.getUsername(), censoredMessage.toString());
        return new ProcessedMessage(channel, moderatedUserMessage, userMessage.getMessage(), isCensored);
    }

    public void setBannedWords(Trie bannedWords) {
        this.bannedWords = bannedWords;
    }
}
