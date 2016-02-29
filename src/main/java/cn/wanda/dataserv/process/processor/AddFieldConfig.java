package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.FieldConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import lombok.Data;

@Data
public class AddFieldConfig {
    @Config
    private String type;
    @Config
    private FieldConfig schema;
    @Config
    private Expression expr;
    @Config(require = RequiredType.OPTIONAL)
    private String before;
    @Config(require = RequiredType.OPTIONAL)
    private String after;
}
