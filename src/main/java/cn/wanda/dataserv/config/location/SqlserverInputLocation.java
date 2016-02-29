package cn.wanda.dataserv.config.location;


import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.SqlserverServer;
import lombok.Data;
import lombok.extern.log4j.Log4j;


@Data
@Log4j
public class SqlserverInputLocation extends LocationConfig {

    @Config(name = "sqlserver")
    private SqlserverServer sqlserver;

    @Config(name = "table", require = RequiredType.OPTIONAL)
    private Expression tbname;

    /**
     * input cols, clause after select in sql statements. default is *
     */
    @Config(name = "cols", require = RequiredType.OPTIONAL)
    private String cols = " * ";

    /**
     * where clause in sql statement
     */
    @Config(name = "condition", require = RequiredType.OPTIONAL)
    private Expression condition;

    /**
     * a custom SQL statement for special requirements<br>
     * defaule is empty<br>
     * if param <b>sql</b> is not blank, we will ignore params <b>tbname</b>,<b>cols</b> and <b>where</b>.
     */
    @Config(name = "sql", require = RequiredType.OPTIONAL)
    private Expression sql;

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