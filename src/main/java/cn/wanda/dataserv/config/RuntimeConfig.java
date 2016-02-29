package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;

/**
 * 运行时参数配置BO
 *
 * @author haobowei
 */
@Data
public class RuntimeConfig {
    /**
     * input reader并发数，默认为10
     */
    @Config(name = "input-concurrency", require = RequiredType.OPTIONAL)
    private Integer inputConcurrency = 10;
    /**
     * output writer并发数，默认为10
     */
    @Config(name = "output-concurrency", require = RequiredType.OPTIONAL)
    private Integer outputConcurrency = 10;
    /**
     * storage类型，默认为内存
     */
    @Config(name = "storage-type", require = RequiredType.OPTIONAL)
    private String storageName = "mem";
    /**
     * storage内存大小的限制，单位是byte
     */
    @Config(name = "byte-limit", require = RequiredType.OPTIONAL)
    private Long byteLimit = -1L;
    /**
     * storage行数限制
     */
    @Config(name = "line-limit", require = RequiredType.OPTIONAL)
    private Integer lineLimit = 1000;
    /**
     * @deprecated 限速
     */
    @Config(name = "limit-rate", require = RequiredType.OPTIONAL)
    private Integer limitRate = -1;
    /**
     * 监控间隔
     */
    @Config(name = "monitor-period", require = RequiredType.OPTIONAL)
    private Integer monitorPeriod = -1;
}
