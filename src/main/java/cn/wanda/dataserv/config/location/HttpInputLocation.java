package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HttpServer;
import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.RequiredType;

@Data
public class HttpInputLocation extends LocationConfig {

    @Config(name = "buffer-size", require = RequiredType.OPTIONAL)
    private int bufferSize = 4 * 1024;

    @Config(name = "http")
    private HttpServer http;

    @Config(name = "url-suffix", require = RequiredType.OPTIONAL)
    private Expression suffix;

    /**
     * 行分隔符, 默认值0x7F表示使用默认\n或\r换行
     */
    private char lineDelim = 0x7F;
}