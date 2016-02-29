package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import lombok.Data;

@Data
public class FilterRowConfig {
    @Config
    private String type;
    @Config
    Expression expr;
}
