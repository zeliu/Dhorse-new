package cn.wanda.dataserv.config;

import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.process.processor.OutputProcessorConfig;

/**
 * dpump的配置对象, 形如:
 * <pre>
 * metadata: inline
 * source:
 *   type: hive
 *   name: table1
 *   field_list:
 *     - field: f1
 *     - type: bigint
 *     - comment
 *   location:
 *     url: a.com
 *     user: u
 *     pass: 123
 *
 * target:
 *   type: hive
 *   name: table2
 *   location:
 *     url: a.com
 *     user: u
 *     pass: 123
 *
 * process:
 *   - type: cut
 *     field_name: [a, b, c]
 *
 * </pre>
 *
 * @author haobowei
 */
@Data
public class DPumpConfig {

    @Config
    List<InputConfig> source;
    @Config
    List<OutputConfig> target;
    @Config(name = "input-processor", require = RequiredType.OPTIONAL, factory = ProcessorConfigFactory.class)
    List<Object> inputProcessor;
    @Config(name = "output-processor", require = RequiredType.OPTIONAL)
    List<OutputProcessorConfig> outputProcessor;
    @Config(require = RequiredType.OPTIONAL)
    RuntimeConfig runtime = new RuntimeConfig();

    public DPumpConfig() {

    }

}
