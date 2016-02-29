package cn.wanda.dataserv.process;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import cn.wanda.dataserv.config.FieldConfig;

@ToString
public class LineSchema {
    LinkedList<FieldConfig> fields = new LinkedList<FieldConfig>();
    Map<String, Integer> index = new HashMap<String, Integer>();
    @Setter
    @Getter
    private String fieldDelim;

    public LineSchema() {
    }

    public void addField(FieldConfig f) {
        fields.add(f);
        index.put(f.getName(), fields.size() - 1);
    }

    public void addField(int i, FieldConfig f) {
        fields.add(i, f);
        refreshIndex();
    }

    public void removeField(String name) {
        fields.remove(checkAndGetIdx(name));
        refreshIndex();
    }

    private int checkAndGetIdx(String name) {
        Integer fidx = index.get(name);
        if (fidx == null) {
            throw new FieldNameNotExistsException(String.format("field: %s not exists", name));
        }
        return fidx;
    }

    public void removeField(String[] names) {
        TreeSet<Integer> idx = new TreeSet<Integer>();
        for (String name : names) {
            idx.add(checkAndGetIdx(name));
        }
        int numRemoved = 0;
        for (Integer i : idx) {
            fields.remove(i - numRemoved);
            numRemoved++;
        }
        refreshIndex();
    }

    public void removeAllField() {
        index.clear();
        fields.clear();
    }

    private void refreshIndex() {
        index.clear();
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i).getName(), i);
        }
    }

    public int getIndex(String name) {
        Integer idx = index.get(name);
        return (idx == null ? -1 : idx);
    }

    public FieldConfig getField(String name) {
        return fields.get(checkAndGetIdx(name));
    }

    public FieldConfig getField(int i) {
        return fields.get(i);
    }

    public LineSchema clone() {
        LineSchema s = new LineSchema();
        s.fields = new LinkedList<FieldConfig>(this.fields);
        s.index = new LinkedHashMap<String, Integer>(this.index);
        s.fieldDelim = this.fieldDelim;
        return s;
    }
    
}
