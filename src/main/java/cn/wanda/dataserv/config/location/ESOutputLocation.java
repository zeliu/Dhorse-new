package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.ESContext;
import cn.wanda.dataserv.config.resource.ESServer;
import cn.wanda.dataserv.core.StoreSchema;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/4/3
 */
@Data
public class ESOutputLocation extends LocationConfig {

    @Config(name = "es-server")
    private ESServer esServer;

    @Config(name = "es-context")
    private ESContext esContext;

    @Config(name = "store_schema", require = RequiredType.OPTIONAL)
    StoreSchema storeSchema = StoreSchema.LINE;
}
