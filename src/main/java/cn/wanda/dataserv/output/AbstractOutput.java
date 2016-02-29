package cn.wanda.dataserv.output;

import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;
import lombok.extern.log4j.Log4j;

@Log4j
public abstract class AbstractOutput implements Output {

    public final static int SUCCESS = 0;
    public final static int FAILURE = 1;

    protected OutputConfig outputConfig;
    protected RuntimeConfig runtimeConfig;

    protected String encoding = "UTF-8";

    public AbstractOutput() {

    }

    @Override
    public void setConfig(OutputConfig outputConfig, RuntimeConfig runtime) {
        this.outputConfig = outputConfig;
        this.runtimeConfig = runtime;
    }

    @Override
    public void post(boolean success) {
        log.info("empty post");
    }

    @Override
    public void prepare() {

    }

    @Override
    public void last(boolean success) {
        log.info("last step");
    }
}