package moderator;

import com.github.tunashred.dtos.UserMessage;
import com.github.tunashred.moderator.ModeratorPack;
import com.github.tunashred.moderator.WordsTrie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CensorTest {
    private final List<WordsTrie> packs = new ArrayList<>();

    @BeforeEach
    void setUp() {
        WordsTrie wordsTrie = new WordsTrie();
        wordsTrie.addWord("bad");
        wordsTrie.addWord("worse");
        wordsTrie.addWord("terrible");
        wordsTrie.addWord("damn");
        wordsTrie.addWord("hell");

        packs.add(wordsTrie);
    }

    // BASIC CASES
    @Test
    @DisplayName("Simple word in middle of sentence")
    void testSimpleWord() {
        assertEquals("*** word here", ModeratorPack.censor(packs, new UserMessage("", "bad word here"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Banned word at end")
    void testWordAtEnd() {
        assertEquals("this is ***", ModeratorPack.censor(packs, new UserMessage("", "this is bad"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Banned word at beginning")
    void testWordAtBeginning() {
        assertEquals("*** start", ModeratorPack.censor(packs, new UserMessage("", "bad start"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Only banned word")
    void testSingleBannedWord() {
        assertEquals("***", ModeratorPack.censor(packs, new UserMessage("", "bad"), " ").getUserMessage().getMessage());
    }

    // CONCATENATED WORDS (Your main question)
    @Test
    @DisplayName("Banned word as prefix in concatenated word")
    void testPrefixConcatenation() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "badword"), "").getUserMessage().getMessage();
        // With your current logic: should be "***word"
        assertEquals("***word", result);
    }

    @Test
    @DisplayName("Banned word as suffix in concatenated word")
    void testSuffixConcatenation() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "wordbad"), "").getUserMessage().getMessage();
        assertEquals("word***", result);
    }

    @Test
    @DisplayName("Banned word in middle of concatenated word")
    void testMiddleConcatenation() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "prebadpost"), "").getUserMessage().getMessage();
        assertEquals("pre***post", result);
    }

    @Test
    @DisplayName("Multiple banned words concatenated")
    void testMultipleConcatenated() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "badworseterrible"), "").getUserMessage().getMessage();
        assertEquals("***********", result); // All three words overlap/adjacent
    }

    // MULTIPLE OCCURRENCES
    @Test
    @DisplayName("Multiple banned words separated by spaces")
    void testMultipleSeparated() {
        assertEquals("*** and ***** news", ModeratorPack.censor(packs, new UserMessage("", "bad and worse news"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Same banned word repeated")
    void testRepeatedWord() {
        assertEquals("*** *** ***", ModeratorPack.censor(packs, new UserMessage("", "bad bad bad"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Same word concatenated")
    void testSameWordConcatenated() {
        assertEquals("******", ModeratorPack.censor(packs, new UserMessage("", "badbad"), " ").getUserMessage().getMessage());
    }

    // OVERLAPPING/CONTAINING PATTERNS
    @Test
    @DisplayName("Word containing banned word")
    void testContainingWord() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "badder"), "").getUserMessage().getMessage();
        assertEquals("***der", result); // Only "bad" part is censored
    }

    @Test
    @DisplayName("Legitimate word containing banned substring")
    void testLegitimateWord() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "badge"), "").getUserMessage().getMessage();
        assertEquals("***ge", result); // This might be undesirable behavior
    }

    // PUNCTUATION BOUNDARIES
    @Test
    @DisplayName("Comma separated banned words")
    void testCommaSeparated() {
        assertEquals("***,*****", ModeratorPack.censor(packs, new UserMessage("", "bad,worse"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Period separated banned words")
    void testPeriodSeparated() {
        assertEquals("***.***", ModeratorPack.censor(packs, new UserMessage("", "bad.worse"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Multiple punctuation")
    void testMultiplePunctuation() {
        assertEquals("***!*****?", ModeratorPack.censor(packs, new UserMessage("", "bad!worse?"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Parentheses around banned word")
    void testParentheses() {
        assertEquals("(***)", ModeratorPack.censor(packs, new UserMessage("", "(bad)"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Apostrophe with banned word")
    void testApostrophe() {
        assertEquals("***'s", ModeratorPack.censor(packs, new UserMessage("", "bad's"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Hyphen separated")
    void testHyphenSeparated() {
        assertEquals("***-*****", ModeratorPack.censor(packs, new UserMessage("", "bad-worse"), " ").getUserMessage().getMessage());
    }

    // WHITESPACE VARIATIONS
    @Test
    @DisplayName("Multiple spaces between words")
    void testMultipleSpaces() {
        assertEquals("***  *****", ModeratorPack.censor(packs, new UserMessage("", "bad  worse"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Tab character separation")
    void testTabSeparation() {
        assertEquals("***\t*****", ModeratorPack.censor(packs, new UserMessage("", "bad\tworse"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Newline separation")
    void testNewlineSeparation() {
        assertEquals("***\n*****", ModeratorPack.censor(packs, new UserMessage("", "bad\nworse"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Leading and trailing spaces")
    void testLeadingTrailingSpaces() {
        assertEquals(" *** ", ModeratorPack.censor(packs, new UserMessage("", " bad "), " ").getUserMessage().getMessage());
    }

    // CASE SENSITIVITY (depends on your trie configuration)
    @Test
    @DisplayName("Uppercase banned word")
    void testUppercase() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "BAD"), " ").getUserMessage().getMessage();
        ;
        // This will pass/fail depending on if your trie is case-sensitive
        // Adjust expectation based on your trie setup
        assertEquals("***", result); // Assuming case-sensitive
    }

    @Test
    @DisplayName("Mixed case banned word")
    void testMixedCase() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "Bad"), " ").getUserMessage().getMessage();
        ;
        assertEquals("***", result); // Assuming case-sensitive
    }

    // SPECIAL CHARACTERS AND NUMBERS
    @Test
    @DisplayName("Banned word with numbers")
    void testWithNumbers() {
        assertEquals("***123", ModeratorPack.censor(packs, new UserMessage("", "bad123"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Numbers before banned word")
    void testNumbersPrefix() {
        assertEquals("123***", ModeratorPack.censor(packs, new UserMessage("", "123bad"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Email-like pattern")
    void testEmailPattern() {
        assertEquals("***@email.com", ModeratorPack.censor(packs, new UserMessage("", "bad@email.com"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Special characters")
    void testSpecialCharacters() {
        assertEquals("$***$", ModeratorPack.censor(packs, new UserMessage("", "$bad$"), " ").getUserMessage().getMessage());
    }

    // EDGE CASES
    @Test
    @DisplayName("Empty string")
    void testEmptyString() {
        assertEquals("", ModeratorPack.censor(packs, new UserMessage("", ""), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Only whitespace")
    void testOnlyWhitespace() {
        assertEquals("   ", ModeratorPack.censor(packs, new UserMessage("", "   "), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("No banned words")
    void testNoBannedWords() {
        assertEquals("good nice fine", ModeratorPack.censor(packs, new UserMessage("", "good nice fine"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Single character")
    void testSingleCharacter() {
        assertEquals("b", ModeratorPack.censor(packs, new UserMessage("", "b"), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Partial banned word")
    void testPartialBannedWord() {
        assertEquals("ba", ModeratorPack.censor(packs, new UserMessage("", "ba"), " ").getUserMessage().getMessage());
    }

    // COMPLEX SCENARIOS
    @Test
    @DisplayName("Long concatenation with multiple banned words")
    void testLongConcatenation() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "superbadworseterribleending"), " ").getUserMessage().getMessage();
        assertEquals("super*******************ending", result);
    }

    @Test
    @DisplayName("Sentence with multiple banned words")
    void testSentenceMultipleBanned() {
        assertEquals("This *** situation is ***** than ********.",
                ModeratorPack.censor(packs, new UserMessage("", "This bad situation is worse than terrible."), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("Repeated pattern with punctuation")
    void testRepeatedWithPunctuation() {
        assertEquals("It's ***, really ***!", ModeratorPack.censor(packs, new UserMessage("", "It's bad, really bad!"), " ").getUserMessage().getMessage());
    }

    // STRESS TESTS
    @Test
    @DisplayName("Very long string with scattered banned words")
    void testLongStringScattered() {
        String input = "The weather is bad today, but yesterday was worse, and tomorrow might be terrible if the forecast is right.";
        String expected = "The weather is *** today, but yesterday was *****, and tomorrow might be ******** if the forecast is right.";
        assertEquals(expected, ModeratorPack.censor(packs, new UserMessage("", input), " ").getUserMessage().getMessage());
    }

    @Test
    @DisplayName("All banned words concatenated")
    void testAllBannedWordsConcatenated() {
        String result = ModeratorPack.censor(packs, new UserMessage("", "badworseterribledamnhell"), " ").getUserMessage().getMessage();
        // This tests how overlapping replacements are handled
        assertEquals("********************", result);
    }
}
