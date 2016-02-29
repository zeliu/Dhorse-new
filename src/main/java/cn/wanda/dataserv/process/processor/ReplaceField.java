package cn.wanda.dataserv.process.processor;

import java.util.regex.Pattern;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.ProcessorSupport;

public class ReplaceField extends ProcessorSupport {

    ReplaceFieldConfig config;
    Pattern pattern;

    @Override
    public void setConfig(Object config) {
        this.config = (ReplaceFieldConfig) config;
        this.pattern = Pattern.compile(this.config.getPattern());
    }

    @Override
    protected LineSchema processSchema(LineSchema schemaAfter) {
        // TODO Auto-generated method stub
        return schemaAfter;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
        int idx = schema.getIndex(config.getFieldName());
        String oriVal = line.getField(idx);
        String newVal = String.valueOf(config.getValue().eval(line));
        String aftRepVal = this.pattern.matcher(oriVal).replaceAll(newVal);
        line.replaceField(idx, aftRepVal);
        return line;
    }
}
