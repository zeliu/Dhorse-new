package cn.wanda.dataserv.config.location;

import lombok.Data;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.utils.DFSUtils;

@Data
public class HdfsOutputLocation extends LocationConfig {

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "buffer-size", require = RequiredType.OPTIONAL)
    private Integer bufferSize = 4 * 1024;

    @Config(name = "gen-manifest", require = RequiredType.OPTIONAL)
    private String genManifest = "true";
    /**
     * target path in hdfs
     */
    @Config(name = "path")
    private Expression path;

    /**
     * 0 delete all files in target directory<br>
     * 1 delete target file before write if exists (if path is end with '/', there will be a default target file name)<br>
     * 2 throw a exception and terminate if target file is exists
     */
    @Config(name = "write-type", require = RequiredType.OPTIONAL)
    private String writeType = "1";

    //TXT_COMP or RCFile
    /**
     * default is org.apache.hadoop.io.compress.DefaultCodec<br>
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
    private String codecClass = DFSUtils.DefaultCodec;

    //SEQ only
    /**
     * include:
     * NONE,BLOCK,RECORD
     */
    @Config(name = "compression-type", require = RequiredType.OPTIONAL)
    private String compressionType = "NONE";
    /**
     * key field index if output file type is sequeuence file <br>
     * defaule is -1, means no key field
     */
    @Config(name = "key-field-index", require = RequiredType.OPTIONAL)
    private Integer keyFieldIndex = -1;
    /**
     * include:
     * txt,seq,txt_comp
     */
    @Config(name = "file-type", require = RequiredType.OPTIONAL)
    private String fileType = DFSUtils.TXT;
    /**
     * key class if output file type is sequeuence file <br>
     * support:<br>
     * Text(defaule)<br>
     * LongWritable<br>
     * IntWritable<br>
     * DoubleWritable<br>
     * BooleanWritable<br>
     * ByteWritable<br>
     * VIntWritable<br>
     * VLongWritable<br>
     * NullWritable<br>
     */
    @Config(name = "key-class", require = RequiredType.OPTIONAL)
    private String keyClass = "org.apache.hadoop.io.Text";
    /**
     * value class if output file type is sequeuence file <br>
     * support:<br>
     * Text(defaule)<br>
     * LongWritable<br>
     * IntWritable<br>
     * DoubleWritable<br>
     * BooleanWritable<br>
     * ByteWritable<br>
     * VIntWritable<br>
     * VLongWritable<br>
     * NullWritable<br>
     */
    @Config(name = "value-class", require = RequiredType.OPTIONAL)
    private String valueClass = "org.apache.hadoop.io.Text";

    //RCFILE only
    @Config(name = "is-compressed", require = RequiredType.OPTIONAL)
    private String isCompressed = "false";
}
