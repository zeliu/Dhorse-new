package cn.wanda.dataserv.config.resource;

import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.ConfigResource;
import lombok.Data;

@Data
public class HdfsConf {

    /**
     * hadoop-site.xml
     */
    @Config(name = "hadoop-conf", require = RequiredType.OPTIONAL)
    private List<ConfEntry> hadoopConf = new ArrayList<ConfEntry>();

    @Config(name = "config-resources", require = RequiredType.OPTIONAL)
    private List<ConfigResource> configResources = new ArrayList<ConfigResource>();
    /**
     * hadoop conf:hadoop.job.ugi
     */
    @Config(name = "ugi", require = RequiredType.OPTIONAL)
    private String ugi;
}