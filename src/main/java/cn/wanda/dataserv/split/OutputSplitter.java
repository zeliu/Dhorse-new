package cn.wanda.dataserv.split;

import java.util.List;

import cn.wanda.dataserv.config.OutputConfig;

public interface OutputSplitter extends Splitter {

    List<OutputConfig> split(OutputConfig outputConfig);
}