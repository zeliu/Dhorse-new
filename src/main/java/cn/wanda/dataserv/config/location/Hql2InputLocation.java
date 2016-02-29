package cn.wanda.dataserv.config.location;

import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.config.resource.HiveServer;

@Data
public class Hql2InputLocation extends LocationConfig {

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "hive", require = RequiredType.OPTIONAL)
    private HiveServer hive = new HiveServer();

    @Config(name = "hql")
    private Expression hql;
}