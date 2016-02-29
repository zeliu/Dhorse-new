package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.MysqlServer;
import lombok.Data;

@Data
public class OracleOutputLocation extends LocationConfig {

    @Config(name = "mysql")
    private MysqlServer mysql;

    @Config(name = "table")
    private Expression tbname;

    /**
     * set clause in load statement
     */
    @Config(name = "set", require = RequiredType.OPTIONAL)
    private String set = "";

    /**
     * REPLACE or IGNORE in load statement
     */
    @Config(name = "replace", require = RequiredType.OPTIONAL)
    private String replace = "REPLACE";

    /**
     * a custom SQL statement before data transmission
     */
    @Config(name = "pre-sql", require = RequiredType.OPTIONAL)
    private String preSql;

    /**
     * a custom SQL statement after data transmission
     */
    @Config(name = "post-sql", require = RequiredType.OPTIONAL)
    private String postSql;
}