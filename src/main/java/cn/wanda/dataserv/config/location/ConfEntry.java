package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

@Data
public class ConfEntry {

    @Config(name = "key")
    private String key;

    @Config(name = "value")
    private String value;
}