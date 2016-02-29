package cn.wanda.dataserv.config.parse;

/**
 * config元素，通用的配置存储BO
 *
 * @author haobowei
 */
public interface ConfigElement {
    public Integer getAsInt();

    public String getAsString();

    public ConfigObject getAsConfigObject();

    public ConfigArray getAsConfigArray();

    public Integer getAsInt(String prop);

    public String getAsString(String prop);

    public ConfigObject getAsConfigObject(String prop);

    public ConfigArray getAsConfigArray(String prop);

    public boolean isNull();

    public Object getObj();
}
