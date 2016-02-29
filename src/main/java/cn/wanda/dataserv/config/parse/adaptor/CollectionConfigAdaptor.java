package cn.wanda.dataserv.config.parse.adaptor;

import java.util.List;
import java.util.Map;

import cn.wanda.dataserv.config.parse.ConfigArray;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.config.parse.ConfigObject;

/**
 * config对象适配器，通过解析Map<List>的结构来构造配置对象
 *
 * @author haobowei
 */
public class CollectionConfigAdaptor implements ConfigElement {
    private Object obj;

    private class CollectionConfigObject extends CollectionConfigAdaptor implements ConfigObject {
        private Map map;

        public CollectionConfigObject(Object config) {
            super(config);
            if (config instanceof Map) {
                this.map = (Map) config;
            } else {
                throw new IllegalStateException("Map needed, but " + config + " found");
            }
        }

        @Override
        public ConfigElement get(String property) {
            return new CollectionConfigAdaptor(map.get(property));
        }

    }

    private class CollectionConfigArray extends CollectionConfigAdaptor implements ConfigArray {
        private List list;

        public CollectionConfigArray(Object config) {
            super(config);
            if (config instanceof List) {
                this.list = (List) config;
            } else {
                throw new IllegalStateException("Map needed, but " + config + " found");
            }
        }


        @Override
        public ConfigElement get(int i) {
            return new CollectionConfigAdaptor(list.get(i));
        }

        @Override
        public int size() {
            return list.size();
        }

    }

    public CollectionConfigAdaptor(Object config) {
        this.obj = config;
    }

    @Override
    public Integer getAsInt() {
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof String) {
            return new Integer((String) obj);
        } else {
            throw new IllegalStateException("integer needed, but " + obj + " found");
        }
    }

    @Override
    public String getAsString() {
        if (obj instanceof Integer) {
            return obj.toString();
        } else if (obj instanceof String) {
            return (String) obj;
        } else if (obj instanceof Boolean) {
            return obj+"";
        }else {
            throw new IllegalStateException("string needed, but " + obj + " found");
        }
    }

    @Override
    public ConfigObject getAsConfigObject() {
        return new CollectionConfigObject(obj);
    }

    @Override
    public ConfigArray getAsConfigArray() {
        return new CollectionConfigArray(obj);
    }

    @Override
    public Integer getAsInt(String prop) {
        return new CollectionConfigObject(obj).get(prop).getAsInt();
    }

    @Override
    public String getAsString(String prop) {
        return new CollectionConfigObject(obj).get(prop).getAsString();
    }

    @Override
    public ConfigObject getAsConfigObject(String prop) {
        return new CollectionConfigObject(obj).get(prop).getAsConfigObject();
    }

    @Override
    public ConfigArray getAsConfigArray(String prop) {
        return new CollectionConfigObject(obj).get(prop).getAsConfigArray();
    }

    @Override
    public boolean isNull() {
        return obj == null;
    }

    @Override
    public Object getObj() {
        return obj;
    }

}
