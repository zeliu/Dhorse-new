package cn.wanda.dataserv.output;

import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;

/**
 * output接口抽象. 不同数据源作为DPump输出时可实现该接口。
 *
 * @author songqian
 */
public interface Output {
    void setConfig(OutputConfig output, RuntimeConfig runtime);

    void writeLine(Line line);

    void close();

    void init();

    void prepare();

    void post(boolean success);

    void last(boolean success);
}
