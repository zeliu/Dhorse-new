package cn.wanda.dataserv.split;

import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.config.OutputConfig;

public abstract class AbstractOutputSplitter implements OutputSplitter {

    public List<OutputConfig> split(OutputConfig outputConfig) {
        List<OutputConfig> outputConfigList = new ArrayList<OutputConfig>();
        outputConfigList.add(outputConfig);
        return outputConfigList;
    }
}