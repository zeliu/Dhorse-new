package cn.wanda.dataserv.input;

import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.core.Line;

/**
 * input接口抽象. 不同数据源作为DPump输入时可实现该接口。
 *
 * @author songqian
 */
public interface Input {
    /**
     * @param input
     * @param runtime
     */
    void setConfig(InputConfig input, RuntimeConfig runtime);

    /**
     *
     */
    void init();

    /**
     * @return
     */
    Line readLine();

    /**
     *
     */
    void close();

    /**
     *
     */
    void prepare();

    /**
     * execute before close
     *
     * @param success if input is success
     */
    void post(boolean success);
}
