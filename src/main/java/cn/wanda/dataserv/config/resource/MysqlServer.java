package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.el.StringConstant;
import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;

@Data
public class MysqlServer {

    @Config(name = "host")
    private Expression host;

    @Config(name = "port", require = RequiredType.OPTIONAL)
    private Expression port = new StringConstant("3306");

    @Config(name = "user")
    private String user;

    @Config(name = "db")
    private Expression db;

    @Config(name = "passwd", require = RequiredType.OPTIONAL)
    private String passwd = "";

    @Config(name = "params", require = RequiredType.OPTIONAL)
    private String params = "";


}
