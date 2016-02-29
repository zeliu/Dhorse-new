package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/2/27
 */
@Data
public class ConfigResource {
    @Config(name = "resource")
    private String resource;
}
