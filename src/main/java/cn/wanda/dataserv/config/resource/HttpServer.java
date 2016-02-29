package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

@Data
public class HttpServer {

    @Config(name = "url")
    private String httpURL;
}