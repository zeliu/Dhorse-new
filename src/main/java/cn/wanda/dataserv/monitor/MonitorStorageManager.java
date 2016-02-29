package cn.wanda.dataserv.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.storage.Storage;
import cn.wanda.dataserv.storage.StorageManager;
import cn.wanda.dataserv.storage.mem.DefaultStorageManager;

public class MonitorStorageManager implements StorageManager {

    private StorageManager sm;

    private long monitorPeriod;

    private Map<String, Monitor> monitors = new ConcurrentHashMap<String, Monitor>();

    public MonitorStorageManager(StorageManager sm, long monitorPeriod) {
        this.sm = sm;
        this.monitorPeriod = monitorPeriod;
    }

    public Line getLine(String id) {
        Line line = sm.getLine(id);
        Monitor m = this.monitors.get(id);
        if (line != null && line.getLine() != null) {
            m.addlOutputNum(1);
            m.addbOutputNum(line.getLine().length());
        }
        return line;
    }

    public int getLine(String id, Line[] lines) {
        int readNum = sm.getLine(id, lines);
        Monitor m = this.monitors.get(id);
        if (readNum > 0) {
            m.addlOutputNum(readNum);
            for (int i = 0; i < readNum; i++) {
                if (lines[i] != null && lines[i].getLine() != null) {
                    m.addbOutputNum(lines[i].getLine().length());
                }
            }
        }
        return readNum;
    }

    public boolean putLine(String id, Line line) {
        boolean result = sm.putLine(id, line);
        Monitor m = this.monitors.get(id);
        if (line != null && line.getLine() != null) {
            m.addlInputNum(1);
            m.addbInputNum(line.getLine().length());
        }
        return result;
    }

    public boolean putLine(String id, Line[] lines, int size) {
        boolean result = sm.putLine(id, lines, size);
        if (DefaultStorageManager.INPUT_ID_ALL.equals(id)) {
            for (Monitor m : this.monitors.values()) {
                m.addlInputNum(size);
                for (int i = 0; i < size; i++) {
                    m.addbInputNum(lines[i].getLine().length());
                }
            }
        } else {
            Monitor m = this.monitors.get(id);
            m.addlInputNum(size);
            for (int i = 0; i < size; i++) {
                m.addbInputNum(lines[i].getLine().length());
            }
        }
        return result;
    }

    public void add(String id, Storage storage) {
        sm.add(id, storage);
        if (this.monitors.get(id) == null) {
            Monitor monitor = new Monitor(id);
            monitor.setPeriod(monitorPeriod);
            this.monitors.put(id, monitor);
        }
    }

    public void remove(String id) {
        sm.remove(id);
        this.monitors.remove(id);
    }

    public Storage get(String inputId) {
        return sm.get(inputId);
    }

    public boolean isEmpty(String id) {
        return sm.isEmpty(id);
    }

    public void closeInput(String id) {
        sm.closeInput(id);
    }

    public void closeInput() {
        sm.closeInput();
    }

    public void setInputLineSchema(LineSchema lineSchema) {
        sm.setInputLineSchema(lineSchema);
    }

    public void print() {
        for (String k : monitors.keySet()) {
            monitors.get(k).printInfoPeriodicity();
            Storage s = sm.get(k);
            if (s != null) {
                s.print();
            }
        }
    }

    public void printTotalInfo() {
        for (String k : monitors.keySet()) {
            monitors.get(k).printTotalInfo();
        }
    }

}