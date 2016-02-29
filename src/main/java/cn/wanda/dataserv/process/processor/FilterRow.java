package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.ProcessorSupport;

public class FilterRow extends ProcessorSupport {

    private FilterRowConfig config;

    @Override
    public void setConfig(Object config) {
        this.config = (FilterRowConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema schemaAfter) {
        // TODO Auto-generated method stub
        return schemaAfter;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
        if (Boolean.TRUE.equals(config.getExpr().eval(line))) {
            return line;
        } else {
            return null;
        }
    }

}
