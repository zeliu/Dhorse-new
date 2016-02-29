package cn.wanda.dataserv.config.location;


import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.config.resource.HiveServer;
import cn.wanda.dataserv.utils.DFSUtils;

@Data
public class Hive2OutputLocation extends LocationConfig {

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "hive", require = RequiredType.OPTIONAL)
    private HiveServer hive = new HiveServer();

    @Config(name = "buffer-size", require = RequiredType.OPTIONAL)
    private int bufferSize = 4 * 1024;

    @Config(name = "table")
    private Expression tableName;

    @Config(name = "db", require = RequiredType.OPTIONAL)
    private String dbName = "default";
    /**
     * format:partFieldName1=val;parFieldName2=val...<br>
     * or "val" if there is only one partition field
     */
    @Config(name = "partition", require = RequiredType.OPTIONAL)
    private Expression partition;

    /**
     * default is none<br>
     * support:<br>
     * org.apache.hadoop.io.compress.DefaultCodec<br>
     * org.apache.hadoop.io.compress.GzipCodec<br>
     * org.apache.hadoop.io.compress.BZip2Codec<br>
     * org.apache.hadoop.io.compress.LzopCodec<br>
     * org.apache.hadoop.io.compress.LzoCodec<br>
     * org.apache.hadoop.io.compress.LzmaCodec<br>
     * org.apache.hadoop.io.compress.QuickLzCodec<br>
     */
    @Config(name = "codec-class", require = RequiredType.OPTIONAL)
    private String codecClass = DFSUtils.TableCodec;

}