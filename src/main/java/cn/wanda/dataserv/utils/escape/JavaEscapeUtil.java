package cn.wanda.dataserv.utils.escape;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;


/**
 * Created by songzhuozhuo on 2015/3/6
 */
final class JavaEscapeUtil {

    private static final char ESCAPE_PREFIX = '\\';
    private static final char ESCAPE_UHEXA_PREFIX2 = 'u';
    private static final char[] ESCAPE_UHEXA_PREFIX = "\\u".toCharArray();

    private static char[] HEXA_CHARS_UPPER = "0123456789ABCDEF".toCharArray();
    private static char[] HEXA_CHARS_LOWER = "0123456789abcdef".toCharArray();


    private static int SEC_CHARS_LEN = '\\' + 1; // 0x5C + 1 = 0x5D
    private static char SEC_CHARS_NO_SEC = '*';
    private static char[] SEC_CHARS;

    private static final char ESCAPE_LEVELS_LEN = 0x9f + 2; // Last relevant char to be indexed is 0x9f
    private static final byte[] ESCAPE_LEVELS;


    static {

        SEC_CHARS = new char[SEC_CHARS_LEN];
        Arrays.fill(SEC_CHARS, SEC_CHARS_NO_SEC);
        SEC_CHARS[0x08] = 'b';
        SEC_CHARS[0x09] = 't';
        SEC_CHARS[0x0A] = 'n';
        SEC_CHARS[0x0C] = 'f';
        SEC_CHARS[0x0D] = 'r';
        SEC_CHARS[0x22] = '"';

        SEC_CHARS[0x27] = '\'';
        SEC_CHARS[0x5C] = '\\';


        ESCAPE_LEVELS = new byte[ESCAPE_LEVELS_LEN];

        Arrays.fill(ESCAPE_LEVELS, (byte) 3);


        for (char c = 0x80; c < ESCAPE_LEVELS_LEN; c++) {
            ESCAPE_LEVELS[c] = 2;
        }


        for (char c = 'A'; c <= 'Z'; c++) {
            ESCAPE_LEVELS[c] = 4;
        }
        for (char c = 'a'; c <= 'z'; c++) {
            ESCAPE_LEVELS[c] = 4;
        }
        for (char c = '0'; c <= '9'; c++) {
            ESCAPE_LEVELS[c] = 4;
        }

        ESCAPE_LEVELS[0x08] = 1;
        ESCAPE_LEVELS[0x09] = 1;
        ESCAPE_LEVELS[0x0A] = 1;
        ESCAPE_LEVELS[0x0C] = 1;
        ESCAPE_LEVELS[0x0D] = 1;
        ESCAPE_LEVELS[0x22] = 1;

        ESCAPE_LEVELS[0x27] = 3;
        ESCAPE_LEVELS[0x5C] = 1;


        for (char c = 0x00; c <= 0x1F; c++) {
            ESCAPE_LEVELS[c] = 1;
        }

        for (char c = 0x7F; c <= 0x9F; c++) {
            ESCAPE_LEVELS[c] = 1;
        }
    }

    private JavaEscapeUtil() {
        super();
    }

    static char[] toUHexa(final int codepoint) {
        final char[] result = new char[4];
        result[3] = HEXA_CHARS_UPPER[codepoint % 0x10];
        result[2] = HEXA_CHARS_UPPER[(codepoint >>> 4) % 0x10];
        result[1] = HEXA_CHARS_UPPER[(codepoint >>> 8) % 0x10];
        result[0] = HEXA_CHARS_UPPER[(codepoint >>> 12) % 0x10];
        return result;
    }

    static String escape(final String text, final JavaEscapeLevel escapeLevel) {

        if (text == null) {
            return null;
        }

        final int level = escapeLevel.getEscapeLevel();

        StringBuilder strBuilder = null;

        final int offset = 0;
        final int max = text.length();

        int readOffset = offset;

        for (int i = offset; i < max; i++) {

            final char c = text.charAt(i);

            final int codepoint;
            if (c < Character.MIN_HIGH_SURROGATE) {
                codepoint = (int) c;
            } else if (Character.isHighSurrogate(c) && (i + 1) < max) {
                final char c1 = text.charAt(i + 1);
                if (Character.isLowSurrogate(c1)) {
                    codepoint = Character.toCodePoint(c, c1);
                } else {
                    codepoint = (int) c;
                }
            } else { // just a normal, single-char, high-valued codepoint.
                codepoint = (int) c;
            }


            if (codepoint <= (ESCAPE_LEVELS_LEN - 2) && level < ESCAPE_LEVELS[codepoint]) {
                continue;
            }


            if (codepoint > (ESCAPE_LEVELS_LEN - 2) && level < ESCAPE_LEVELS[ESCAPE_LEVELS_LEN - 1]) {

                if (Character.charCount(codepoint) > 1) {
                    // This is to compensate that we are actually escaping two char[] positions with a single codepoint.
                    i++;
                }
                continue;
            }


            if (strBuilder == null) {
                strBuilder = new StringBuilder(max + 20);
            }

            if (i - readOffset > 0) {
                strBuilder.append(text, readOffset, i);
            }

            if (Character.charCount(codepoint) > 1) {
                i++;
            }

            readOffset = i + 1;


            if (codepoint < SEC_CHARS_LEN) {
                // We will try to use a SEC

                final char sec = SEC_CHARS[codepoint];

                if (sec != SEC_CHARS_NO_SEC) {
                    // SEC found! just write it and go for the next char
                    strBuilder.append(ESCAPE_PREFIX);
                    strBuilder.append(sec);
                    continue;
                }

            }


            System.out.println(codepoint);
            if (Character.charCount(codepoint) > 1) {
                final char[] codepointChars = Character.toChars(codepoint);
                strBuilder.append(ESCAPE_UHEXA_PREFIX);
                strBuilder.append(toUHexa(codepointChars[0]));
                strBuilder.append(ESCAPE_UHEXA_PREFIX);
                strBuilder.append(toUHexa(codepointChars[1]));
                continue;
            }

            strBuilder.append(ESCAPE_UHEXA_PREFIX);
            strBuilder.append(toUHexa(codepoint));

        }


        if (strBuilder == null) {
            return text;
        }

        if (max - readOffset > 0) {
            strBuilder.append(text, readOffset, max);
        }

        return strBuilder.toString();

    }

    /**
     * *
     *
     * @param text
     * @param start
     * @param end
     * @param radix
     * @return
     */
    static int parseIntFromReference(final String text, final int start, final int end, final int radix) {
        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = text.charAt(i);
            int n = -1;
            for (int j = 0; j < HEXA_CHARS_UPPER.length; j++) {
                if (c == HEXA_CHARS_UPPER[j] || c == HEXA_CHARS_LOWER[j]) {
                    n = j;
                    break;
                }
            }
            result = (radix * result) + n;
        }
        return result;
    }


    /**
     * * @param text
     *
     * @param start
     * @param end
     * @param radix
     * @return
     */
    static int parseIntFromReference(final char[] text, final int start, final int end, final int radix) {
        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = text[i];
            int n = -1;
            for (int j = 0; j < HEXA_CHARS_UPPER.length; j++) {
                if (c == HEXA_CHARS_UPPER[j] || c == HEXA_CHARS_LOWER[j]) {
                    n = j;
                    break;
                }
            }
            result = (radix * result) + n;
        }
        return result;
    }


    /**
     * *
     *
     * @param text
     * @param start
     * @param end
     * @return
     */
    static boolean isOctalEscape(final String text, final int start, final int end) {

        if (start >= end) {
            return false;
        }

        final char c1 = text.charAt(start);
        if (c1 < '0' || c1 > '7') {
            return false;
        }

        if (start + 1 >= end) {
            return (c1 != '0'); // It would not be an octal escape, but the U+0000 escape sequence.
        }

        final char c2 = text.charAt(start + 1);
        if (c2 < '0' || c2 > '7') {
            return (c1 != '0'); // It would not be an octal escape, but the U+0000 escape sequence.
        }

        if (start + 2 >= end) {
            return (c1 != '0' || c2 != '0'); // It would not be an octal escape, but the U+0000 escape sequence + '0'.
        }

        final char c3 = text.charAt(start + 2);
        if (c3 < '0' || c3 > '7') {
            return (c1 != '0' || c2 != '0'); // It would not be an octal escape, but the U+0000 escape sequence + '0'.
        }

        return (c1 != '0' || c2 != '0' || c3 != '0'); // Check it's not U+0000 (escaped) + '00'

    }

    /**
     * *
     *
     * @param text
     * @param start
     * @param end
     * @return
     */
    static boolean isOctalEscape(final char[] text, final int start, final int end) {

        if (start >= end) {
            return false;
        }

        final char c1 = text[start];
        if (c1 < '0' || c1 > '7') {
            return false;
        }

        if (start + 1 >= end) {
            return (c1 != '0');
        }

        final char c2 = text[start + 1];
        if (c2 < '0' || c2 > '7') {
            return (c1 != '0');
        }

        if (start + 2 >= end) {
            return (c1 != '0' || c2 != '0');
        }

        final char c3 = text[start + 2];
        if (c3 < '0' || c3 > '7') {
            return (c1 != '0' || c2 != '0');
        }

        return (c1 != '0' || c2 != '0' || c3 != '0');

    }


    static String unicodeUnescape(final String text) {

        if (text == null) {
            return null;
        }

        StringBuilder strBuilder = null;

        final int offset = 0;
        final int max = text.length();

        int readOffset = offset;
        int referenceOffset = offset;

        for (int i = offset; i < max; i++) {

            final char c = text.charAt(i);

            /*
             * Check the need for an unescape operation at this point
             */

            if (c != ESCAPE_PREFIX || (i + 1) >= max) {
                continue;
            }

            int codepoint = -1;

            if (c == ESCAPE_PREFIX) {

                final char c1 = text.charAt(i + 1);

                if (c1 == ESCAPE_UHEXA_PREFIX2) {
                    // This can be a uhexa escape, we need exactly four more characters

                    int f = i + 2;
                    // First, discard any additional 'u' characters, which are allowed
                    while (f < max) {
                        final char cf = text.charAt(f);
                        if (cf != ESCAPE_UHEXA_PREFIX2) {
                            break;
                        }
                        f++;
                    }
                    int s = f;
                    // Parse the hexadecimal digits
                    while (f < (s + 4) && f < max) {
                        final char cf = text.charAt(f);
                        if (!((cf >= '0' && cf <= '9') || (cf >= 'A' && cf <= 'F') || (cf >= 'a' && cf <= 'f'))) {
                            break;
                        }
                        f++;
                    }

                    if ((f - s) < 4) {
                        i++;
                        continue;
                    }

                    codepoint = parseIntFromReference(text, s, f, 16);

                    referenceOffset = f - 1;

                } else {
                    i++;
                    continue;
                }
            }

            if (strBuilder == null) {
                strBuilder = new StringBuilder(max + 5);
            }

            if (i - readOffset > 0) {
                strBuilder.append(text, readOffset, i);
            }

            i = referenceOffset;
            readOffset = i + 1;

            if (codepoint > '\uFFFF') {
                strBuilder.append(Character.toChars(codepoint));
            } else {
                strBuilder.append((char) codepoint);
            }

        }


        if (strBuilder == null) {
            return text;
        }

        if (max - readOffset > 0) {
            strBuilder.append(text, readOffset, max);
        }

        return strBuilder.toString();

    }


    static boolean requiresUnicodeUnescape(final char[] text, final int offset, final int len) {

        if (text == null) {
            return false;
        }

        final int max = (offset + len);

        for (int i = offset; i < max; i++) {

            final char c = text[i];

            if (c != ESCAPE_PREFIX || (i + 1) >= max) {
                continue;
            }

            if (c == ESCAPE_PREFIX) {

                final char c1 = text[i + 1];

                if (c1 == ESCAPE_UHEXA_PREFIX2) {
                    return true;
                }

            }

        }

        return false;

    }


    static void unicodeUnescape(final char[] text, final int offset, final int len, final Writer writer)
            throws IOException {

        if (text == null) {
            return;
        }

        final int max = (offset + len);

        int readOffset = offset;
        int referenceOffset = offset;

        for (int i = offset; i < max; i++) {

            final char c = text[i];

            /*
             * Check the need for an unescape operation at this point
             */

            if (c != ESCAPE_PREFIX || (i + 1) >= max) {
                continue;
            }

            int codepoint = -1;

            if (c == ESCAPE_PREFIX) {
                final char c1 = text[i + 1];
                if (c1 == ESCAPE_UHEXA_PREFIX2) {
                    int f = i + 2;
                    while (f < max) {
                        final char cf = text[f];
                        if (cf != ESCAPE_UHEXA_PREFIX2) {
                            break;
                        }
                        f++;
                    }
                    int s = f;
                    // Parse the hexadecimal digits
                    while (f < (s + 4) && f < max) {
                        final char cf = text[f];
                        if (!((cf >= '0' && cf <= '9') || (cf >= 'A' && cf <= 'F') || (cf >= 'a' && cf <= 'f'))) {
                            break;
                        }
                        f++;
                    }

                    if ((f - s) < 4) {
                        i++;
                        continue;
                    }

                    codepoint = parseIntFromReference(text, s, f, 16);
                    referenceOffset = f - 1;
                } else {
                    i++;
                    continue;

                }

            }

            if (i - readOffset > 0) {
                writer.write(text, readOffset, (i - readOffset));
            }

            i = referenceOffset;
            readOffset = i + 1;


            if (codepoint > '\uFFFF') {
                writer.write(Character.toChars(codepoint));
            } else {
                writer.write((char) codepoint);
            }

        }

        if (max - readOffset > 0) {
            writer.write(text, readOffset, (max - readOffset));
        }

    }


    static String unescape(final String text) {

        if (text == null) {
            return null;
        }

        // Will be exactly the same object if no unicode escape was needed
        final String unicodeEscapedText = unicodeUnescape(text);

        StringBuilder strBuilder = null;

        final int offset = 0;
        final int max = unicodeEscapedText.length();

        int readOffset = offset;
        int referenceOffset = offset;

        for (int i = offset; i < max; i++) {

            final char c = unicodeEscapedText.charAt(i);


            if (c != ESCAPE_PREFIX || (i + 1) >= max) {
                continue;
            }

            int codepoint = -1;

            if (c == ESCAPE_PREFIX) {

                final char c1 = unicodeEscapedText.charAt(i + 1);

                switch (c1) {
                    case '0':
                        if (!isOctalEscape(unicodeEscapedText, i + 1, max)) {
                            codepoint = 0x00;
                            referenceOffset = i + 1;
                        }
                        ;
                        break;
                    case 'b':
                        codepoint = 0x08;
                        referenceOffset = i + 1;
                        break;
                    case 't':
                        codepoint = 0x09;
                        referenceOffset = i + 1;
                        break;
                    case 'n':
                        codepoint = 0x0A;
                        referenceOffset = i + 1;
                        break;
                    case 'f':
                        codepoint = 0x0C;
                        referenceOffset = i + 1;
                        break;
                    case 'r':
                        codepoint = 0x0D;
                        referenceOffset = i + 1;
                        break;
                    case '"':
                        codepoint = 0x22;
                        referenceOffset = i + 1;
                        break;
                    case '\'':
                        codepoint = 0x27;
                        referenceOffset = i + 1;
                        break;
                    case '\\':
                        codepoint = 0x5C;
                        referenceOffset = i + 1;
                        break;
                }

                if (codepoint == -1) {

                    if (c1 >= '0' && c1 <= '7') {

                        int f = i + 2;
                        while (f < (i + 4) && f < max) { // We need only a max of two more chars
                            final char cf = unicodeEscapedText.charAt(f);
                            if (!(cf >= '0' && cf <= '7')) {
                                break;
                            }
                            f++;
                        }

                        codepoint = parseIntFromReference(unicodeEscapedText, i + 1, f, 8);

                        if (codepoint > 0xFF) {
                            codepoint = parseIntFromReference(unicodeEscapedText, i + 1, f - 1, 8);
                            referenceOffset = f - 2;
                        } else {
                            referenceOffset = f - 1;
                        }

                    } else {

                        i++;
                        continue;
                    }
                }

            }

            if (strBuilder == null) {
                strBuilder = new StringBuilder(max + 5);
            }

            if (i - readOffset > 0) {
                strBuilder.append(unicodeEscapedText, readOffset, i);
            }

            i = referenceOffset;
            readOffset = i + 1;

            if (codepoint > '\uFFFF') {
                strBuilder.append(Character.toChars(codepoint));
            } else {
                strBuilder.append((char) codepoint);
            }

        }


        if (strBuilder == null) {
            return unicodeEscapedText;
        }

        if (max - readOffset > 0) {
            strBuilder.append(unicodeEscapedText, readOffset, max);
        }

        return strBuilder.toString();

    }

    static void unescape(final char[] text, final int offset, final int len, final Writer writer)
            throws IOException {

        if (text == null) {
            return;
        }

        char[] unicodeEscapedText = text;
        int unicodeEscapedOffset = offset;
        int unicodeEscapedLen = len;
        if (requiresUnicodeUnescape(text, offset, len)) {
            final CharArrayWriter charArrayWriter = new CharArrayWriter(len + 2);
            unicodeUnescape(text, offset, len, charArrayWriter);
            unicodeEscapedText = charArrayWriter.toCharArray();
            unicodeEscapedOffset = 0;
            unicodeEscapedLen = unicodeEscapedText.length;
        }

        final int max = (unicodeEscapedOffset + unicodeEscapedLen);

        int readOffset = unicodeEscapedOffset;
        int referenceOffset = unicodeEscapedOffset;

        for (int i = unicodeEscapedOffset; i < max; i++) {

            final char c = unicodeEscapedText[i];

            if (c != ESCAPE_PREFIX || (i + 1) >= max) {
                continue;
            }

            int codepoint = -1;

            if (c == ESCAPE_PREFIX) {

                final char c1 = unicodeEscapedText[i + 1];

                switch (c1) {
                    case '0':
                        if (!isOctalEscape(unicodeEscapedText, i + 1, max)) {
                            codepoint = 0x00;
                            referenceOffset = i + 1;
                        }
                        ;
                        break;
                    case 'b':
                        codepoint = 0x08;
                        referenceOffset = i + 1;
                        break;
                    case 't':
                        codepoint = 0x09;
                        referenceOffset = i + 1;
                        break;
                    case 'n':
                        codepoint = 0x0A;
                        referenceOffset = i + 1;
                        break;
                    case 'f':
                        codepoint = 0x0C;
                        referenceOffset = i + 1;
                        break;
                    case 'r':
                        codepoint = 0x0D;
                        referenceOffset = i + 1;
                        break;
                    case '"':
                        codepoint = 0x22;
                        referenceOffset = i + 1;
                        break;
                    case '\'':
                        codepoint = 0x27;
                        referenceOffset = i + 1;
                        break;
                    case '\\':
                        codepoint = 0x5C;
                        referenceOffset = i + 1;
                        break;
                }

                if (codepoint == -1) {

                    if (c1 >= '0' && c1 <= '7') {

                        int f = i + 2;
                        while (f < (i + 4) && f < max) { // We need only a max of two more chars
                            final char cf = unicodeEscapedText[f];
                            if (!(cf >= '0' && cf <= '7')) {
                                break;
                            }
                            f++;
                        }

                        codepoint = parseIntFromReference(unicodeEscapedText, i + 1, f, 8);

                        if (codepoint > 0xFF) {
                            codepoint = parseIntFromReference(unicodeEscapedText, i + 1, f - 1, 8);
                            referenceOffset = f - 2;
                        } else {
                            referenceOffset = f - 1;
                        }

                    } else {

                        i++;
                        continue;

                    }

                }

            }


            if (i - readOffset > 0) {
                writer.write(unicodeEscapedText, readOffset, (i - readOffset));
            }

            i = referenceOffset;
            readOffset = i + 1;


            if (codepoint > '\uFFFF') {
                writer.write(Character.toChars(codepoint));
            } else {
                writer.write((char) codepoint);
            }

        }

        if (max - readOffset > 0) {
            writer.write(unicodeEscapedText, readOffset, (max - readOffset));
        }


    }

    public static void main(String[] args) {
        System.out.println("\\001");
        System.out.println(unicodeUnescape("\\001"));

    }

}

