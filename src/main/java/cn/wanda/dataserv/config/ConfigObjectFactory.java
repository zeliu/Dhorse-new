package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.parse.ConfigElement;

/**
 * 创建Object的工厂类，在annotation中使用
 *
 * @author haobowei
 */
public interface ConfigObjectFactory {
    /**
     * @param ce        parser解析的ConfigElement对象
     * @param intention 意向类
     * @return 创建后的对象
     */
    Object create(ConfigElement ce, Class intention);
}
