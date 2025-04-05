package kafka.inputs;

public class Dataset {
    public static final String[] VALID_USERNAMES = {"gulie", "baboi", "alecs", "tim", "tom", "This_dude", "other_Dude", "Some_Other_Dude", "s0m30n3__", "i.am.here"};

    public static final String[] INVALID_USERNAMES = {"", " ", ".", "/", ";", ":", "visit@me.net", "www.enash.com"};

    public static final String[] GROUP_NAMES = {"trage-baboiul", "albinutele-vesele", "cafelutsa-cu-peste", "chat-munca", "cadou-invatatoare-2017"};

    public static final String[] BANNED_WORDS = {"septari", "banana", "salar", "mai lasi din pret", "bomboana", "parizer", "seminte fenicul"};

    public static final String[] GOOD_MESSAGES = {
            "hey there",
            "how you doing?",
            "\"''''i'm finey heeeeere3....you?",
            "artificial chat here",
            "oh wow this does not s333333m good atall.",
            "ba, nana ce faci?",
            "boombaoala",
            "just more messages",
            "for dummy stuff which ideally should also check for way too big messages",
            "but the idea about messages is that they should also be welllll written",
            "though some ppl just",
            "write",
            "sentces",
            "sentenecs*",
            "SENTENCES IM FAT HITTING THE KEYBOARD",
            "awfulllllllllllr j249it 98itg wn4et5w 9sn5rot hn"
    };

    public static final String[] BAD_MESSAGES = {
            BANNED_WORDS[3] + " din " + BANNED_WORDS[0] + "i aia?",
            "    ,.fdsf" + BANNED_WORDS[3] + "some words here too ??:{}>{>",
            BANNED_WORDS[2] + BANNED_WORDS[1],
            BANNED_WORDS[6] + BANNED_WORDS[5] + BANNED_WORDS[4],
            BANNED_WORDS[0] + " la baboi" + BANNED_WORDS[5] + " " + BANNED_WORDS[3] + "some sticky string here"
    };
}
