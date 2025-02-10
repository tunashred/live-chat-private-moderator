package com.github.tunashred.moderator;

import com.github.tunashred.dtos.MessageInfo;
import com.github.tunashred.privatedtos.ProcessedMessage;
import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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

    public static void main(String[] args) {
        Moderator moderator = new Moderator("packs/banned.txt");
        MessageInfo messageInfo = new MessageInfo(null, null, "");
        ProcessedMessage output = moderator.censor(messageInfo);
        System.out.println(output.getProcessedMessage());
    }

    public ProcessedMessage censor(MessageInfo messageInfo) {
        String message = messageInfo.getMessage();
        StringBuilder censoredMessage = new StringBuilder(message);
        boolean isCensored = false;

        for (Emit emit : bannedWords.parseText(message)) {
            int start = emit.getStart(), end = emit.getEnd();
            String replacement = "*".repeat(emit.getKeyword().length());
            censoredMessage.replace(start, end + 1, replacement);
            isCensored = true;
        }
        return new ProcessedMessage(messageInfo, censoredMessage.toString(), isCensored);
    }
}
