package cn.wanda.dataserv.config.resource;

import lombok.Data;

import cn.wanda.dataserv.config.annotation.Config;

@Data
public class MongoServer {

    @Config(name = "uri")
    private String uri;
}