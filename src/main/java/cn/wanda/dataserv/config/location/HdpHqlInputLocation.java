package cn.wanda.dataserv.config.location;

import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.config.resource.HiveServer;

@Data
public class HdpHqlInputLocation extends LocationConfig {

    @Config(name = "hdfs")
    private HdfsConf hdfs;

    @Config(name = "hive")
    private HiveServer hive;

    @Config(name = "hql")
    private Expression hql;
}