package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.MongoServer;
import lombok.Data;

@Data
public class MongoOutputLocation extends LocationConfig {

    @Config(name = "mongo")
    private MongoServer mongo;

    /**
     * database name, optional, priority selection than database name in uri(mongoserver)
     */
    @Config(name = "database", require = RequiredType.OPTIONAL)
    private Expression database;

    /**
     * collection name, optional, priority selection than collection name in uri(mongoserver)
     */
    @Config(name = "collection", require = RequiredType.OPTIONAL)
    private Expression collection;

    @Config(name = "is-append", require = RequiredType.OPTIONAL)
    private String isAppend = "false";

}