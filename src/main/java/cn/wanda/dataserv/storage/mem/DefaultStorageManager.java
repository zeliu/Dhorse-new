package cn.wanda.dataserv.storage.mem;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.storage.Storage;
import cn.wanda.dataserv.storage.StorageManager;

public class DefaultStorageManager implements StorageManager {

    public static final String INPUT_ID_ALL = "ALL";
    private Map<String, Storage> storageMap = new ConcurrentHashMap<String, Storage>();

    @Override
    public Line getLine(String inputId) {
        return storageMap.get(inputId).getLine();
    }

    @Override
    public boolean putLine(String id, Line line) {
        if (INPUT_ID_ALL.equals(id)) {
            for (Storage s : storageMap.values()) {
                s.putLine(line);
            }
        } else {
            storageMap.get(id).putLine(line);
        }
        return true;
    }

    @Override
    public int getLine(String id, Line[] lines) {
        return storageMap.get(id).getLine(lines);
    }

    @Override
    public boolean putLine(String id, Line[] lines, int size) {
        if (INPUT_ID_ALL.equals(id)) {
            for (Storage s : storageMap.values()) {
                s.putLine(lines, size);
            }
        } else {
            storageMap.get(id).putLine(lines, size);
        }
        return true;
    }

    @Override
    public void add(String id, Storage storage) {
        if (storageMap.containsKey(id)) {
            throw new DuplicateStorageException();
        }
        storageMap.put(id, storage);
    }

    @Override
    public void remove(String id) {
        this.storageMap.remove(id);
    }

    @Override
    public boolean isEmpty(String id) {
        return storageMap.get(id).isEmpty();
    }

    @Override
    public void closeInput(String id) {
        Storage s = storageMap.get(id);
        if (s != null) {
            s.setPutCompleted(true);
        }
    }

    @Override
    public void closeInput() {
        for (Storage s : storageMap.values()) {
            s.setPutCompleted(true);
        }
    }

    @Override
    public void setInputLineSchema(LineSchema lineSchema) {

    }

    @Override
    public Storage get(String id) {
        if (INPUT_ID_ALL.equals(id)) {
            Iterator<Storage> i = storageMap.values().iterator();
            if (i.hasNext()) {
                return i.next();
            }
            return null;
        }
        return storageMap.get(id);
    }

    @Override
    public void print() {
        for (String k : storageMap.keySet()) {
            storageMap.get(k).print();
        }
    }

    @Override
    public void printTotalInfo() {
    }
}
