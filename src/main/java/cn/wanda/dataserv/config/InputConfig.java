package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

/**
 * 输入配置对象BO
 *
 * @author haobowei
 */
@Data
public class InputConfig {
    @Config
    private String id;
    /**
     * 输入schema
     */
    @Config(require = RequiredType.OPTIONAL)
    private SchemaConfig schema = new SchemaConfig();
    /**
     * 输入location
     */
    @Config(factory = InputLocationConfigFactory.class)
    private LocationConfig location;
    /**
     * 输入编码
     */
    @Config(require = RequiredType.OPTIONAL)
    private String encode = "UTF-8";
}
