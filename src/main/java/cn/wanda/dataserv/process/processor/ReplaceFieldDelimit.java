package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.ProcessorSupport;
import lombok.extern.log4j.Log4j;

/**
 * Created by songzhuozhuo on 2015/3/4
 */
@Log4j
public class ReplaceFieldDelimit extends ProcessorSupport {
    private ReplaceFieldDelimitConfig config;

    @Override
    public void setConfig(Object config) {
        this.config = (ReplaceFieldDelimitConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema schemaAfter) {
        return schemaAfter;
    }

    @Override
    protected LineWrapper processLine(LineWrapper line) {
        line.setFieldDelim(config.getFieldDelimit());
        return line;
    }
}
