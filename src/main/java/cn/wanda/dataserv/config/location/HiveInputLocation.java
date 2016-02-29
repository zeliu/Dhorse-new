package cn.wanda.dataserv.config.location;


import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.CompositeExpression;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.ConfigParseException;
import lombok.Data;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.config.resource.HiveServer;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.DFSUtils;
import cn.wanda.dataserv.utils.HiveUtils;

@Data
@Log4j
public class HiveInputLocation extends LocationConfig {

    private static Map<Class<? extends InputFormat>, String> hiveDfsTypeMap;

    static {
        hiveDfsTypeMap = new HashMap<Class<? extends InputFormat>, String>();
        hiveDfsTypeMap.put(RCFileInputFormat.class, DFSUtils.RCFILE);
        hiveDfsTypeMap.put(TextInputFormat.class, DFSUtils.TXT);
        hiveDfsTypeMap.put(SequenceFileInputFormat.class, DFSUtils.SEQ);
    }

    private static final String codecparamkey = "io.compression.codecs";

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "hive", require = RequiredType.OPTIONAL)
    private HiveServer hive = new HiveServer();

    @Config(name = "buffer-size", require = RequiredType.OPTIONAL)
    private Integer bufferSize = 4 * 1024;

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
     * 把hiveinputlocation转化为对应的若干hdfsinputlocation
     */
    @Override
    public List<Object> doSplit() {
        String tableNameString = ExpressionUtils.getOneValue(tableName);
        if (StringUtils.isBlank(tableNameString)) {
            throw new InputException("table name is empty!");
        }
        String partitionString = ExpressionUtils.getOneValue(partition);
        // get hive conf
        HiveConf conf;
        try {
            conf = HiveUtils.getConf(hdfs.getUgi(), hive.getHiveConf(), hdfs.getHadoopConf());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new ConfigParseException(String.format(
                    "Initialize hive conf failed:%s,%s", e.getMessage(),
                    e.getCause()));
        }

        // get hive instance and table
        Hive hive;
        Table t;
        try {
            hive = Hive.get(conf);
            t = hive.getTable(dbName, tableNameString, true);
        } catch (HiveException e) {
            log.error(e.getMessage(), e);
            throw new ConfigParseException(String.format(
                    "Initialize hive conf failed:%s,%s", e.getMessage(),
                    e.getCause()));
        }

        //get hdfs path
        Map<String, String> partitionMap = HiveUtils.calcPartitions(t, partitionString);
        try {
            String path = "";

            if (partitionMap.size() > 0) {
                Partition p = hive.getPartition(t, partitionMap, false);
                path = p.getLocation();
            } else {
                path = t.getTTable().getSd().getLocation();
            }

            Configuration c = DFSUtils.getConf(path, hdfs.getUgi(), hdfs);

            List<String> pathList = DFSUtils.getPathList(path, c);

            //get hdfs file type
            Class<? extends InputFormat> ofClazz = t.getInputFormatClass();
            String dfsType = hiveDfsTypeMap.get(ofClazz);

            Map<String, String> tParams = t.getParameters();

            String codecClass = "";
            if (tParams != null) {
                codecClass = tParams.get(codecparamkey);
                if (StringUtils.isNotBlank(codecClass)) {
                    dfsType = DFSUtils.TXT_COMP;
                }
            }

            List<Object> result = new ArrayList<Object>();
            if (pathList.size() == 0) {
                throw new InputException("source data is empty.");
            }
            for (String subPath : pathList) {
                HdfsInputLocation dfsLocation = new HdfsInputLocation();

                dfsLocation.setBufferSize(bufferSize);
                HdfsConf h = new HdfsConf();
                h.setUgi(hdfs.getUgi());
                h.setHadoopConf(hdfs.getHadoopConf());
                dfsLocation.setHdfs(h);
                dfsLocation.setPath(new CompositeExpression(null, true, subPath));
                dfsLocation.setFileType(dfsType);
                dfsLocation.setCodecClass(codecClass);

                result.add(dfsLocation);
            }
            return result;
        } catch (HiveException e) {
            throw new ConfigParseException("Hive input split failed!", e);
        } catch (IOException e) {
            throw new ConfigParseException("Hive input split failed!", e);
        } catch (URISyntaxException e) {
            throw new ConfigParseException("Hive input split failed!", e);
        }
    }
}