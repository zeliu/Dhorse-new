package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;

@Data
public class RemoveFieldConfig {
    @Config
    private String type;
    @Config(name = "field-name")
    private String fieldName;
}
