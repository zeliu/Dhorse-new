package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

/**
 * Created by liuze on 2015/10/29
 */
@Data
public class HbaseServer {

    @Config(name = "quorum")
    private String quorum;

    @Config(name = "port")
    private String port;

    @Config(name = "parent")
    private String parent;


}
