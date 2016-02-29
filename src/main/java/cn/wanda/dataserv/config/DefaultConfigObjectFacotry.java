package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;

/**
 * 默认的配置对象工厂类
 *
 * @author haobowei
 */
public class DefaultConfigObjectFacotry implements ConfigObjectFactory {

    /**
     * 直接创建意向类的新实例
     */
    @Override
    public Object create(ConfigElement ce, Class intention) {
        try {
            return intention.newInstance();
        } catch (Exception e) {
            throw new ConfigParseException(e);
        }
    }

}
