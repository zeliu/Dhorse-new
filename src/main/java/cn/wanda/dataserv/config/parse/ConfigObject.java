package cn.wanda.dataserv.config.parse;

/**
 * Config对象
 *
 * @author haobowei
 */
public interface ConfigObject extends ConfigElement {
    public ConfigElement get(String property);
}
