package cn.wanda.dataserv.config;

import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

/**
 * schema配置BO
 *
 * @author haobowei
 */
@Data
public class SchemaConfig {
    @Config
    private String name;
    @Config(require = RequiredType.OPTIONAL)
    private List<FieldConfig> fields;
    /**
     * 列分割符
     */
    @Config(name = "field-delim", require = RequiredType.OPTIONAL)
    private String fieldDelim = "\t";
    /**
     * 行分割符
     */
    @Config(name = "line-delim", require = RequiredType.OPTIONAL)
    private String lineDelim = "\n";
}
