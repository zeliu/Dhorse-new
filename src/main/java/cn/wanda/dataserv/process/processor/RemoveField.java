package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.ProcessorSupport;

public class RemoveField extends ProcessorSupport {

    private RemoveFieldConfig config;

    @Override
    public void setConfig(Object config) {
        this.config = (RemoveFieldConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema schemaAfter) {
        schemaAfter.removeField(config.getFieldName());
        return schemaAfter;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
        line.removeField(schema.getIndex(config.getFieldName()));
        return line;
    }

}
