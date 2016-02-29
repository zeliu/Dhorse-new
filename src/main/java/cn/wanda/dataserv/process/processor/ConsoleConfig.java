package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

@Data
public class ConsoleConfig {
    @Config
    private String type;
}
