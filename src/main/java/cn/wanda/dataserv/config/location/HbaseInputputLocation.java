package cn.wanda.dataserv.config.location;

import lombok.Data;
import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.resource.HbaseConf;
import cn.wanda.dataserv.config.resource.HbaseServer;

/**
 * Created by liuze on 2015/10/29
 */
@Data
public class HbaseInputputLocation extends LocationConfig {

    @Config(name = "hbase-server")
    private HbaseServer hbaseServer;

    @Config(name = "hbase-conf")
    private HbaseConf hbaseConf;


}
