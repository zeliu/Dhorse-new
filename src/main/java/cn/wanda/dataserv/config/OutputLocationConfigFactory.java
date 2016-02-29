package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;

/**
 * 输出配置BO
 *
 * @author haobowei
 */
public class OutputLocationConfigFactory implements ConfigObjectFactory {

    @Override
    public Object create(ConfigElement ce, Class intention) {
        try {
            String type = ce.getAsString("type");
            ConfigTypeVals configType = ConfigTypeVals.getByType(type);
            if (configType == null)
                throw new ConfigParseException(String.format(
                        "type %s is invalid", type));
            return configType.outputLocationClass.newInstance();
        } catch (Exception e) {
            throw new ConfigParseException(e);
        }
    }
}
