package cn.wanda.dataserv.config;

import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

/**
 * 字段对象BO
 *
 * @author haobowei
 */
@Data
public class FieldConfig {
    /**
     * 字段名
     */
    @Config
    private String name;
    /**
     * 字段类型
     */
    @Config
    private String type;
    /**
     * 字段验证器
     */
    /**
     * TODO : validation
     */
    @Config(require = RequiredType.OPTIONAL)
    private FieldValidation validation = new FieldValidation();
    /**
     * 字段注释
     */
    @Config(require = RequiredType.OPTIONAL)
    private String comment = "";
}
