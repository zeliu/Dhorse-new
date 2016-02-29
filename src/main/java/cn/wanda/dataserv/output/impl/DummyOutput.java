package cn.wanda.dataserv.output.impl;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.Output;

/**
 * For test and analyse data
 *
 * @author songqian
 */
public class DummyOutput extends AbstractOutput implements Output {
    @Override
    public void writeLine(Line line) {

    }

    @Override
    public void close() {

    }

    @Override
    public void init() {

    }
}