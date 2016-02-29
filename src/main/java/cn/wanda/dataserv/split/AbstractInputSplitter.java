package cn.wanda.dataserv.split;

import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.config.InputConfig;

public abstract class AbstractInputSplitter implements InputSplitter {

    public List<InputConfig> split(InputConfig inputConfig) {
        List<InputConfig> inputConfigList = new ArrayList<InputConfig>();
        inputConfigList.add(inputConfig);
        return inputConfigList;
    }
}