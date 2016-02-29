package cn.wanda.dataserv.utils.escape;

import java.io.IOException;
import java.io.Writer;


/**
 * Created by songzhuozhuo on 2015/3/6
 */
public final class JavaEscape {

    public static String escapeJavaMinimal(final String text) {
        return escapeJava(text, JavaEscapeLevel.LEVEL_1_BASIC_ESCAPE_SET);
    }


    public static String escapeJava(final String text) {
        return escapeJava(text, JavaEscapeLevel.LEVEL_2_ALL_NON_ASCII_PLUS_BASIC_ESCAPE_SET);
    }


    public static String escapeJava(final String text, final JavaEscapeLevel level) {

        if (level == null) {
            throw new IllegalArgumentException("The 'level' argument cannot be null");
        }

        return JavaEscapeUtil.escape(text, level);

    }

    public static String unescapeJava(final String text) {
        return JavaEscapeUtil.unescape(text);
    }


    public static void unescapeJava(final char[] text, final int offset, final int len, final Writer writer)
            throws IOException {
        if (writer == null) {
            throw new IllegalArgumentException("Argument 'writer' cannot be null");
        }

        final int textLen = (text == null ? 0 : text.length);

        if (offset < 0 || offset > textLen) {
            throw new IllegalArgumentException(
                    "Invalid (offset, len). offset=" + offset + ", len=" + len + ", text.length=" + textLen);
        }

        if (len < 0 || (offset + len) > textLen) {
            throw new IllegalArgumentException(
                    "Invalid (offset, len). offset=" + offset + ", len=" + len + ", text.length=" + textLen);
        }

        JavaEscapeUtil.unescape(text, offset, len, writer);

    }


    private JavaEscape() {
        super();
    }


}

