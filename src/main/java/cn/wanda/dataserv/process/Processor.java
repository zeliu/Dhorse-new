package cn.wanda.dataserv.process;


public interface Processor {
    void setConfig(Object config);

    LineSchema process(LineSchema lineSchema);

    LineWrapper process(LineWrapper line);
}
