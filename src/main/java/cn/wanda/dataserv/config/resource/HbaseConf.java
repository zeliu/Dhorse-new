package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import lombok.Data;

/**
 * Created by liuze on 2015/10/29
 */
@Data
public class HbaseConf {

    @Config(name = "table")
    private String table;

    @Config(name = "family")
    private String family;
    
    @Config(name = "columns")
    private String columns;
    
    @Config(name = "rowkey-delim", require = RequiredType.OPTIONAL)
    private String rowkeyDelim;
    
    @Config(name = "rowkey-location", require = RequiredType.OPTIONAL)
    private String rowkeyLocation;
    
    @Config(name = "value-location", require = RequiredType.OPTIONAL)
    private String valueLocation;

    @Config(name = "filter-family", require = RequiredType.OPTIONAL)
    private String filterFamily;

    @Config(name = "filter-column", require = RequiredType.OPTIONAL)
    private String filterColumn;

    @Config(name = "begin-date", require = RequiredType.OPTIONAL)
    private String beginDate;

    @Config(name = "end-date", require = RequiredType.OPTIONAL)
    private String endDate;


}
