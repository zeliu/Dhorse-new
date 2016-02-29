package cn.wanda.dataserv.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author songqian
 */
public class StringUtil {

    public static final String EMPTY = "";

    public static final String DPUMP_FIELD_DELIM = "\t";

    /**
     * @param str
     * @param separator
     * @return
     */
    public static List<String> simpleSplit(String str, String separator) {
        if (str == null || str.length() == 0) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        int pos = str.length();
        int lastpos = str.indexOf(separator);
        int separatorlength = separator.length();
        if (lastpos < 0) {
            result.add(str);
            return result;
        } else {
            result.add(str.substring(0, lastpos));
        }

        while ((pos = str.indexOf(separator, lastpos + separatorlength)) != -1) {

            result.add(str.substring(lastpos + separatorlength, pos));

            lastpos = pos;
        }
        result.add(str.substring(lastpos + separatorlength, str.length()));
        return result;
    }

    /**
     * @param collection
     * @param separator
     * @return
     */
    public static String join(Collection<String> collection, String separator, int capacity) {

        Iterator<String> iterator = collection.iterator();
        // handle null, zero and one elements before building a buffer
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            return EMPTY;
        }
        String first = iterator.next();
        if (!iterator.hasNext()) {
            return first;
        }

        // two or more elements
        StringBuffer buf = new StringBuffer(capacity); // Java default is 16, probably too small
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            String obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        return buf.toString();
    }
}