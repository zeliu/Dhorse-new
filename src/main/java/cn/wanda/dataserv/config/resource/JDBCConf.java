package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import lombok.Data;

/**
 * Created by liuze on 2015/10/30 0030.
 */

@Data
public class JDBCConf {

    @Config(name = "presql")
    private String preSql;

    @Config(name = "updatesql")
    private String updateSql;

    @Config(name = "field-delim")
    private String fieldDelim;

    @Config(name = "batch-size")
    private String batchSize;

}
