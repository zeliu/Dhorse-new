package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.LocalFile;
import lombok.Data;

@Data
public class LocalFileLocation extends LocationConfig {

    /**
     * local file resource
     */
    @Config(name = "file", require = RequiredType.OPTIONAL)
    private LocalFile fileResource;

    /**
     * file path suffix
     */
    @Config(name = "file-path")
    private Expression filePath;

    @Config(name = "gen-md5", require = RequiredType.OPTIONAL)
    private String genMd5 = "true";
}
