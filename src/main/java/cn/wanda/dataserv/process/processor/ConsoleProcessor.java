package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.Processor;
import org.apache.commons.lang.ObjectUtils;

import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.process.Processor;

public class ConsoleProcessor implements Processor {

    @Override
    public LineSchema process(LineSchema lineSchema) {
        System.out.println(ObjectUtils.toString(lineSchema));
        return lineSchema;
    }

    @Override
    public LineWrapper process(LineWrapper line) {
        System.out.println(ObjectUtils.toString(line));
        return line;
    }

    @Override
    public void setConfig(Object config) {
        // TODO Auto-generated method stub

    }

}
