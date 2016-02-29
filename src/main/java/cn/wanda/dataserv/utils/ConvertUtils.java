package cn.wanda.dataserv.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.com.esotericsoftware.kryo.util.ObjectMap;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by songzhuozhuo on 2015/4/1
 */
public class ConvertUtils {
    /**
     * @param value
     * @param defaultValue
     * @return
     */
    public static TimeValue getAsTime(String value, TimeValue defaultValue) {
        return TimeValue.parseTimeValue(value, defaultValue);
    }


    public static String get(String val1, String val2) {
        return val1 != null ? val1 : val2;
    }


    public static Boolean getAsBoolean(String val, Boolean defultVal) {
        if (StringUtils.isEmpty(val)) {
            return defultVal;
        }
        return Boolean.parseBoolean(val);
    }

    public static Integer getAsInt(String sValue, Integer defaultValue) {
        if (StringUtils.isEmpty(sValue)) {
            return defaultValue;
        } else {
            try {
                return Integer.valueOf(Integer.parseInt(sValue));
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ByteSizeValue getAsBytesSize(String value, ByteSizeValue defaultValue) throws SettingsException {
        return ByteSizeValue.parseBytesSizeValue(value, defaultValue);
    }


    /**
     * read map form json
     *
     * @param str
     * @return
     */
    public static <T> T readFromJsonStr(String str, Class<T> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(str, clazz);
    }

    public static String writeToJsonStr(Object value) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(value);
    }


    public static void main(String[] args) throws IOException {

    }
}
