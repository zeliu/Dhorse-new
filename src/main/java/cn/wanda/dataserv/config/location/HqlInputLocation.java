package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HiveServer;
import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.HdfsConf;

@Data
public class HqlInputLocation extends LocationConfig {

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "hive", require = RequiredType.OPTIONAL)
    private HiveServer hive = new HiveServer();

    @Config(name = "hql")
    private Expression hql;
}