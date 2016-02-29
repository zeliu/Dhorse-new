package cn.wanda.dataserv.core;

import java.util.Map;

public interface ConfigLoader {

    Map load(String filePath);
}