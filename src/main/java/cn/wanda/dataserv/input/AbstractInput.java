package cn.wanda.dataserv.input;

import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;

public abstract class AbstractInput implements Input {

    public final static int SUCCESS = 0;
    public final static int FAILURE = 1;

    protected InputConfig inputConfig;
    protected RuntimeConfig runtimeConfig;

    protected String encoding = "UTF-8";


    public AbstractInput() {

    }

    @Override
    public void setConfig(InputConfig inputConfig, RuntimeConfig runtime) {
        this.inputConfig = inputConfig;
        this.runtimeConfig = runtime;
    }

    @Override
    public void post(boolean success) {

    }

    @Override
    public void prepare() {

    }
}