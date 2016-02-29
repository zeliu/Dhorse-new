package cn.wanda.dataserv.core;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private Map<String, Object> valueMap = new HashMap<String, Object>();

    public void put(String key, Object value) {
        valueMap.put(key, value);
    }

    public Object get(String key) {
        return valueMap.get(key);
    }

    public void put(Map valueMap) {
        if (valueMap == null || valueMap.size() == 0) {
            return;
        }
        this.valueMap.putAll(valueMap);
    }

    public Map get() {
        return valueMap;
    }

}
