package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.StringConstant;
import lombok.Data;
import cn.wanda.dataserv.config.annotation.Config;

/**
 * Created by liuze on 2015/10/29
 */
@Data
public class JDBCServer {


    @Config(name = "host")
    private String host;

    @Config(name = "port")
    private String port;

    @Config(name = "user")
    private String user;

    @Config(name = "db")
    private String db;

    @Config(name = "passwd", require = RequiredType.OPTIONAL)
    private String passwd = "";

    @Config(name = "autocommit", require = RequiredType.OPTIONAL)
    private boolean autocommit = true;

}
