package com.github.tunashred.moderator;

import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

import java.util.List;

public class ModeratorPack {
    static public ProcessedMessage censor(List<WordsTrie> packs, UserMessage userMessage, String channel) {
        String message = userMessage.getMessage();
        StringBuilder censoredMessage = new StringBuilder(message);
        boolean isCensored = false;

        for (WordsTrie bannedWordsPack : packs) {
            Trie trie = bannedWordsPack.getTrie();
            for (Emit emit : trie.parseText(message)) {
                int start = emit.getStart(), end = emit.getEnd();
                String replacement = "*".repeat(emit.getKeyword().length());
                censoredMessage.replace(start, end + 1, replacement);
                isCensored = true;
            }
            message = censoredMessage.toString();
        }

        UserMessage moderatedUserMessage = new UserMessage(userMessage.getUsername(), censoredMessage.toString());
        return new ProcessedMessage(channel, moderatedUserMessage, userMessage.getMessage(), isCensored);
    }
}
