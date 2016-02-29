package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;

import lombok.Data;

@Data
public class LocalFile {

    @Config(name = "file-path-prefix")
    private String filePathPrefix;
}