package cn.wanda.dataserv.config;

import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

/**
 * 输出配置BO
 *
 * @author haobowei
 */
@Data
public class OutputConfig {
    @Config
    private String id;
    /**
     * 输出schema
     */
    @Config(require = RequiredType.OPTIONAL)
    private SchemaConfig schema = new SchemaConfig();
    /**
     * 输出地址
     */
    @Config(factory = OutputLocationConfigFactory.class)
    private LocationConfig location;
    /**
     * 输出编码
     */
    @Config(require = RequiredType.OPTIONAL)
    private String encode = "UTF-8";

}
