package cn.wanda.dataserv.config.resource;

import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.location.ConfigResource;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.location.ConfEntry;

@Data
public class HiveServer {

    /**
     * hive-site.xml
     */
    @Config(name = "hive-conf", require = RequiredType.OPTIONAL)
    private List<ConfEntry> hiveConf = new ArrayList<ConfEntry>();

    @Config(name = "config-resources", require = RequiredType.OPTIONAL)
    private List<ConfigResource> configResources = new ArrayList<ConfigResource>();
}