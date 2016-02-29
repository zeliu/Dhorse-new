package cn.wanda.dataserv.config.resource;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/4/1
 */

@Data
public class JDBCContext {
    @Config(name = "url")
    private String url;

    @Config(name = "user")
    private String user;

    @Config(name = "password")
    private String password;

    @Config(name = "sql")
    private String sql;

    @Config(name = "autocommit", require = RequiredType.OPTIONAL)
    private boolean autocommit = false;

    @Config(name = "fetchsize", require = RequiredType.OPTIONAL)
    private String fetchsize;

    @Config(name = "rounding", require = RequiredType.OPTIONAL)
    private String rounding = null;

    @Config(name = "scale", require = RequiredType.OPTIONAL)
    private int scale = 2;

    @Config(name = "max_rows", require = RequiredType.OPTIONAL)
    private int maxRows = 0;

    @Config(name = "max_retries", require = RequiredType.OPTIONAL)
    private int maxRetries = 3;

    @Config(name = "max_retries_wait", require = RequiredType.OPTIONAL)
    private String maxRetriesWait = "30s";

    @Config(name = "ignore_null_values", require = RequiredType.OPTIONAL)
    private boolean ignoreNullValues = false;

    @Config(name = "column_name_map", require = RequiredType.OPTIONAL)
    private String columnNameMap = null;

    @Config(name = "connection_properties", require = RequiredType.OPTIONAL)
    private String connectionProperties = null;

    @Config(name = "query_timeOut", require = RequiredType.OPTIONAL)
    private String queryTimeOut = "1800s";

    private boolean isTimestampDiffSupported;

    @Config(name = "treat_binary_as_tring", require = RequiredType.OPTIONAL)
    private boolean treatBinaryAsString = true;

    @Config(name = "fields_separator", require = RequiredType.OPTIONAL)
    private String fieldsSeparator = "\t";
}
