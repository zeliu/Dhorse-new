package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.resource.JDBCConf;
import cn.wanda.dataserv.config.resource.JDBCServer;
import lombok.Data;

/**
 * Created by liuze on 2015/10/30 0030.
 */

@Data
public class JDBCOutputLocation extends LocationConfig {
    @Config(name = "jdbc-conf")
    private JDBCConf jdbcConf;

    @Config(name = "db-type")
    private String type;

    @Config(name = "jdbc-server")
    private JDBCServer jdbcServer;

}
