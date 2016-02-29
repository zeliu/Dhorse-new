package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.Expression;

import lombok.Data;

@Data
public class DtsConf {

    @Config(name = "item")
    private Expression item;
}