package cn.wanda.dataserv.config.location;

import lombok.Data;
import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.resource.EXCELConf;

/**
 * Created by liuze on 2015/10/30 0030.
 */

@Data
public class EXCELOutputLocation extends LocationConfig {
    @Config(name = "excel-conf")
    private EXCELConf excelConf;


}
