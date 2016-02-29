package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

import lombok.Data;

/**
 * 字段对象验证BO
 *
 * @author haobowei
 */
@Data
public class FieldValidation {
    /**
     * 正则;范围
     */
    @Config(require = RequiredType.OPTIONAL)
    private String type = "";
    /**
     * 验证表达式
     */
    @Config(require = RequiredType.OPTIONAL)
    private String expr = "";
}
