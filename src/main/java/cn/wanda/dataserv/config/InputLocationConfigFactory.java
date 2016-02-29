package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;

/**
 * InputLocation的工厂类，根据type构造对象
 *
 * @author haobowei
 */
public class InputLocationConfigFactory implements ConfigObjectFactory {

    @Override
    public Object create(ConfigElement ce, Class intention) {
        try {
            String type = ce.getAsString("type");
            ConfigTypeVals configType = ConfigTypeVals.getByType(type);
            if (configType == null)
                throw new ConfigParseException(String.format(
                        "type %s is invalid", type));
            return configType.inputLocationClass
                    .newInstance();
        } catch (Exception e) {
            throw new ConfigParseException(e);
        }
    }

}
