package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.ProcessorSupport;

public class LineToJSON extends ProcessorSupport {
    private LineToJSONConfig config;

    @Override
    public void setConfig(Object config) {
        this.config = (LineToJSONConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema schema) {
        return schema;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
    	line.toJSONLine(config,schema);
    	return line;
    }
  
}
