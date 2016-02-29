package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import lombok.Data;

@Data
public class ReplaceFieldConfig {
    @Config
    private String type;
    @Config(name = "field-name")
    private String fieldName;
    @Config
    private String pattern;
    @Config
    private Expression value;
}
