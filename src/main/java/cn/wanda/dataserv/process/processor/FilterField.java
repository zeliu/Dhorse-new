package cn.wanda.dataserv.process.processor;

import java.util.ArrayList;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.ProcessorSupport;

public class FilterField extends ProcessorSupport {
    private FilterFieldConfig config;

    @Override
    public void setConfig(Object config) {
        this.config = (FilterFieldConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema schemaAfter) {
        schemaAfter.removeAllField();
        for (String fieldName : config.getFieldNames()) {
            schemaAfter.addField(schema.getField(fieldName));
        }
        return schemaAfter;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
        LineWrapper newLine = line.newFrom();
        newLine.setFields(new ArrayList<String>());
        newLine.setSplitted(true);
        for (String fieldName : config.getFieldNames()) {
            newLine.addField(line.getField(schema.getIndex(fieldName)));
        }
        return newLine;
    }
}
