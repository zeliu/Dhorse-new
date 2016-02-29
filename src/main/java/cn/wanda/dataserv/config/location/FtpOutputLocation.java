package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.FtpServer;
import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;

@Data
public class FtpOutputLocation extends LocationConfig {

    @Config(name = "ftp")
    private FtpServer ftp;

    /**
     * ftp文件名<br>
     * 使用时会和ftp中path拼接成完整路径<br>
     * 若以"/"结尾，则认为为目录，无目标文件<br>
     * input时若为目录，则会传输目录下所有文件<br>
     */
    @Config(name = "file-name")
    private Expression fileName;

}