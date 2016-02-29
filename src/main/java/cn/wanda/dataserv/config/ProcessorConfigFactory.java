package cn.wanda.dataserv.config;

import java.util.HashMap;
import java.util.Map;

import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.process.processor.*;
import cn.wanda.dataserv.config.parse.ConfigElement;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.process.processor.AddFieldConfig;
import cn.wanda.dataserv.process.processor.ConsoleConfig;
import cn.wanda.dataserv.process.processor.FilterFieldConfig;
import cn.wanda.dataserv.process.processor.FilterRowConfig;
import cn.wanda.dataserv.process.processor.RemoveFieldConfig;
import cn.wanda.dataserv.process.processor.ReplaceFieldConfig;

/**
 * Processor的配置工厂
 *
 * @author haobowei
 */
public class ProcessorConfigFactory implements ConfigObjectFactory {

    //TODO: 抽取置配置文件
    Map<String, Class> processorClassMap = new HashMap<String, Class>();

    {
        processorClassMap.put("add-field", AddFieldConfig.class);
        processorClassMap.put("filter-field", FilterFieldConfig.class);
        processorClassMap.put("filter-row", FilterRowConfig.class);
        processorClassMap.put("remove-field", RemoveFieldConfig.class);
        processorClassMap.put("replace-field", ReplaceFieldConfig.class);
        processorClassMap.put("console", ConsoleConfig.class);
        processorClassMap.put("replace-delimit", ReplaceFieldDelimitConfig.class);
        processorClassMap.put("line-to-json", LineToJSONConfig.class);
    }

    @Override
    public Object create(ConfigElement ce, Class intention) {
        String type = ce.getAsString("type");
        Class configClass = processorClassMap.get(type);
        if (configClass == null) {
            throw new ConfigParseException(String.format("processor type %s is not found", type));
        }
        try {
            return processorClassMap.get(ce.getAsString("type")).newInstance();
        } catch (Exception e) {
            throw new ConfigParseException(e);
        }
    }

}
