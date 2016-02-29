package cn.wanda.dataserv.split;

import java.util.List;

import cn.wanda.dataserv.config.InputConfig;

public interface InputSplitter extends Splitter {

    List<InputConfig> split(InputConfig inputConfig);
}