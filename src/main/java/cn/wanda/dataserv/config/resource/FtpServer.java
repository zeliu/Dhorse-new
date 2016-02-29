package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.RequiredType;

@Data
public class FtpServer {
    /**
     * 参考格式: ftp://ip:port/path
     */
    @Config(name = "ip")
    private String ip;

    /**
     * 端口，默认为21
     */
    @Config(name = "port", require = RequiredType.OPTIONAL)
    private String port = "21";

    /**
     * 用户名，默认为anonymous
     */
    @Config(name = "user", require = RequiredType.OPTIONAL)
    private String user = "anonymous";

    /**
     * 密码，默认为anonymous
     */
    @Config(name = "passwd", require = RequiredType.OPTIONAL)
    private String passwd = "anonymous";

    /**
     *
     */
    @Config(name = "path", require = RequiredType.OPTIONAL)
    private String path = "";
}