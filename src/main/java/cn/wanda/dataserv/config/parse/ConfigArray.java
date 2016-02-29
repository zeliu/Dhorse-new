package cn.wanda.dataserv.config.parse;

/**
 * Config数组
 *
 * @author haobowei
 */
public interface ConfigArray extends ConfigElement {
    public ConfigElement get(int i);

    public int size();
}
