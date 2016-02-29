package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/4/3
 */
@Data
public class ESContext {

    @Config(name = "index")
    private String index;

    @Config(name = "type")
    private String type;
    
    @Config(name = "index-id", require = RequiredType.OPTIONAL)
    private String indexId;
    
    @Config(name = "index-id-delim", require = RequiredType.OPTIONAL)
    private String indexIdDelim;
    
    @Config(name = "index-covered", require = RequiredType.OPTIONAL)
    private String indexCovered = "false";
    
    @Config(name = "parent", require = RequiredType.OPTIONAL)
    private String parent;

    private boolean shouldAutoGenerateId = true;

    @Config(name = "sniff", require = RequiredType.OPTIONAL)
    private String sniff;

    @Config(name = "timeout", require = RequiredType.OPTIONAL)
    private String timeout;

    @Config(name = "max_bulk_actions", require = RequiredType.OPTIONAL)
    private String maxBulkActions;

    @Config(name = "max_concurrent_bulk_requests", require = RequiredType.OPTIONAL)
    private String maxConcurrentBulkRequests;

    @Config(name = "max_bulk_volume", require = RequiredType.OPTIONAL)
    private String maxBulkVolume = "5m";

    @Config(name = "max_request_wait", require = RequiredType.OPTIONAL)
    private String maxRequestWait = "60s";

    @Config(name = "flush_interval", require = RequiredType.OPTIONAL)
    private String flushInterval = "5s";

    @Config(name = "index_settings", require = RequiredType.OPTIONAL)
    private String indexSettings;

    @Config(name = "type_mapping", require = RequiredType.OPTIONAL)
    private String typeMapping;
}
