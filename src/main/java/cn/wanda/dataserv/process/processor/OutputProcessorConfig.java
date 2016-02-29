package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.ProcessorConfigFactory;
import cn.wanda.dataserv.config.annotation.Config;

import lombok.Data;

/**
 * 输出处理器配置
 *
 * @author haobowei
 */
@Data
public class OutputProcessorConfig {

    @Config
    private String id;

    @Config(name = "processor", factory = ProcessorConfigFactory.class)
    private Object processorConfig;
}