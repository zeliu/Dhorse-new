package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.ESContext;
import cn.wanda.dataserv.config.resource.ESServer;
import cn.wanda.dataserv.config.resource.HbaseConf;
import cn.wanda.dataserv.config.resource.HbaseServer;
import cn.wanda.dataserv.core.StoreSchema;
import lombok.Data;

/**
 * Created by liuze on 2015/10/29
 */
@Data
public class HbaseOutputLocation extends LocationConfig {

    @Config(name = "hbase-server")
    private HbaseServer hbaseServer;

    @Config(name = "hbase-conf")
    private HbaseConf hbaseConf;


}
