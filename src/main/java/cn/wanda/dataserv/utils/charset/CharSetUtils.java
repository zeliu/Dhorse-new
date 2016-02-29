package cn.wanda.dataserv.utils.charset;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;


public class CharSetUtils {

    public static CharsetDecoder getDecoderForName(String charsetName) {

        return Charset.forName(charsetName).newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
}