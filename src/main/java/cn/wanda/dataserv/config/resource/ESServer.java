package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

@Data
public class ESServer {

    @Config(name = "host")
    private String host;

    @Config(name = "port")
    private String port;

    @Config(name = "cluster")
    private String cluster;


}
