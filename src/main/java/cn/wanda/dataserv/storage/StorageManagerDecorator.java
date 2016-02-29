package cn.wanda.dataserv.storage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.Processor;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.Processor;
import lombok.extern.log4j.Log4j;

@Log4j
public class StorageManagerDecorator implements StorageManager {
    StorageManager sm;
    private List<Processor> inputProcessorList;
    private Map<String, List<Processor>> outputProcessorMap;
    private LineSchema inputLineSchema;
    private LineSchema outputLineSchema;

    public StorageManagerDecorator(StorageManager sm,
                                   List<Processor> inputProcessorList,
                                   Map<String, List<Processor>> outputProcessorList) {
        this.sm = sm;
        this.inputProcessorList = inputProcessorList;
        this.outputProcessorMap = outputProcessorList;

    }

    @Override
    public Line getLine(String id) {
        Line line = sm.getLine(id);
        if (Line.EOF != line) {
            List<Processor> outputProcessorList = outputProcessorMap.get(id);
            if (outputProcessorList != null) {
                LineWrapper lw = new LineWrapper();
                lw.setLine(line);
                lw.split(outputLineSchema);
                for (Processor p : outputProcessorList) {
                    lw = p.process(lw);
                }
                return lw.getLine();
            }
            return line;
        } else {
            return line;
        }
    }

    @Override
    public boolean putLine(String id, Line line) {
        if (Line.EOF != line && inputProcessorList.size() > 0) {
            LineWrapper lw = new LineWrapper();
            lw.setLine(line);
            lw.split(inputLineSchema);
            for (Processor p : inputProcessorList) {
                lw = p.process(lw);
            }
            return sm.putLine(id, lw.getLine());
        } else {
            return sm.putLine(id, line);
        }
    }

    @Override
    public int getLine(String id, Line[] lines) {
        int readnum = sm.getLine(id, lines);
        List<Processor> outputProcessorList = outputProcessorMap.get(id);
        if (outputProcessorList != null) {
            for (int i = 0; i < readnum; i++) {
                Line line = lines[i];
                if (Line.EOF != line) {
                    LineWrapper lw = new LineWrapper();
                    lw.setLine(line);
                    if (inputLineSchema != null) {
                        lw.setFieldDelim(inputLineSchema.getFieldDelim());
                    }
                    lw.split(outputLineSchema);
                    lw.split(outputLineSchema);
                    for (Processor p : outputProcessorList) {
                        lw = p.process(lw);
                    }
                    lines[i] = lw.getLine();
                }
            }
        }
        return readnum;
    }

    @Override
    public boolean putLine(String id, Line[] lines, int size) {
        if (inputProcessorList.size() > 0) {
            for (int i = 0; i < size; i++) {
                Line line = lines[i];
                if (Line.EOF != line) {
                    LineWrapper lw = new LineWrapper();
                    lw.setLine(line);
                    lw.split(inputLineSchema);
                    for (Processor p : inputProcessorList) {
                        lw = p.process(lw);
                    }
                    lines[i] = lw.getLine();
                }
            }
        }
        return sm.putLine(id, lines, size);
    }

    @Override
    public void add(String id, Storage storage) {
        sm.add(id, storage);
    }

    @Override
    public void remove(String id) {
        sm.remove(id);
    }

    @Override
    public boolean isEmpty(String id) {
        return sm.isEmpty(id);
    }

    @Override
    public void closeInput(String id) {
        sm.closeInput(id);
    }

    @Override
    public void closeInput() {
        sm.closeInput();
    }

    @Override
    public void setInputLineSchema(LineSchema lineSchema) {
        this.inputLineSchema = lineSchema;
        LineSchema ls = inputLineSchema;
        for (Processor p : inputProcessorList) {
            ls = p.process(ls);
        }
        this.outputLineSchema = ls;
        for (Iterator<Entry<String, List<Processor>>> i = outputProcessorMap.entrySet().iterator(); i.hasNext(); ) {
            List<Processor> outputProcessorList = i.next().getValue();
            ls = this.outputLineSchema;
            for (Processor p : outputProcessorList) {
                ls = p.process(ls);
            }
        }
    }

    @Override
    public Storage get(String inputId) {
        // TODO Auto-generated method stub
        return sm.get(inputId);
    }

    @Override
    public void print() {
        sm.print();
    }

    @Override
    public void printTotalInfo() {
        sm.printTotalInfo();
    }
}
