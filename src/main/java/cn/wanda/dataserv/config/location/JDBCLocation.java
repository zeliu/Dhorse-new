package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.JDBCContext;
import cn.wanda.dataserv.core.StoreSchema;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/4/1
 */

@Data
public class JDBCLocation extends LocationConfig {
    @Config(name = "jdbc")
    private JDBCContext context;

    @Config(name = "sql", require = RequiredType.OPTIONAL)
    StoreSchema storeSchema = StoreSchema.LINE;
}
