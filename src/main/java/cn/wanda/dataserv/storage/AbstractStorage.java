package cn.wanda.dataserv.storage;

import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.config.RuntimeConfig;

public abstract class AbstractStorage implements Storage {

    protected boolean putCompleted;

    protected String id;

    @Override
    public void setPutCompleted(boolean close) {
        this.putCompleted = close;
    }

    @Override
    public boolean init(RuntimeConfig rc, String id) {
        this.id = id;
        putCompleted = false;
        return true;
    }
}