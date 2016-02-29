package cn.wanda.dataserv.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.ConfigResource;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.config.resource.HiveServer;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import cn.wanda.dataserv.config.location.ConfEntry;

/**
 * @author songqian
 */
@Log4j
public class HiveUtils {

    public final static String defaule_hive_config = "config/hive-default.xml";
    public final static String defaule_hive_storage_config = "config/hive-storagehandlers.xml";
    public final static String hdp_hive_config = "config/hdphive-default.xml";

    private final static String partition_splitter = ";";
    private final static String partition_equal = "=";

    public static final Integer BHIVE = 1;
    public static final Integer BHIVE2 = 4;
    public static final Integer HHIVE = 2;
    public static final Integer CHIVE = 3;

    /**
     * Get {@link Configuration}.
     *
     * @param ugi        ugi
     * @param hiveConf   hive-site.xml path
     * @param hadoopConf hadoop-site.xml path
     * @return {@link Configuration}
     * @throws IOException
     */

    public static HiveConf getConf(String ugi, List<ConfEntry> hiveConf, List<ConfEntry> hadoopConf)
            throws IOException {
        return getConf(ugi, hiveConf, hadoopConf, BHIVE);
    }

    public static HiveConf getConf(String ugi, List<ConfEntry> hiveConf, List<ConfEntry> hadoopConf, Integer hiveType)
            throws IOException {

        HiveConf cfg = null;

        if (cfg == null) {
            cfg = new HiveConf();

            cfg.setClassLoader(DFSUtils.class.getClassLoader());
            //add default hadoop config
            if (hiveType == BHIVE) {
                //add default hadoop config
                cfg.addResource(new Path(DFSUtils.default_hadoop_config));
                //add default hadoop config
                cfg.addResource(new Path(defaule_hive_config));

                if (hadoopConf == null || hadoopConf.size() == 0) {
                    // try to load hadoop-site from HADOOP_HOME
                    String hadoopDir = System.getenv("HADOOP_HOME");
                    log.info("HADOOP_HOME:" + hadoopDir);
                    if (null != hadoopDir) {
                        //run in hadoop 2(baidu internal version)
                        if (new File(hadoopDir + "/conf/hadoop-site.xml").exists()) {
                            cfg.addResource(new Path(hadoopDir + "/conf/hadoop-site.xml"));
                        }
                    }
                }

                if (hiveConf == null || hiveConf.size() == 0) {
                    // try to load hive-site from HIVE_HOME
                    String hiveDir = System.getenv("HIVE_HOME");
                    log.info("HIVE_HOME:" + hiveDir);
                    if (null != hiveDir) {
                        //run in hive-2.x(baidu internal version)
                        if (new File(hiveDir + "/conf/hive-site.xml").exists()) {
                            cfg.addResource(new Path(hiveDir + "/conf/hive-site.xml"));
                        }
                    }
                }
            } else if (hiveType == BHIVE2) {
                //add default hadoop config
                cfg.addResource(new Path(DFSUtils.default_hadoop_config));
                //add default hadoop config
                cfg.addResource(new Path(defaule_hive_config));
                cfg.addResource(new Path(defaule_hive_storage_config));
                cfg.set("hive.metastore.local", "true");
                System.setProperty("hive.root.mode", "true");
                cfg.set("hive.metastore.rawstore.impl",
                        "org.apache.hadoop.hive.metastore.ObjectStore");
                cfg.set("hive.storagehandler.session",
                        "org.apache.hadoop.hive.storagehandlers.session.SessionStorageHandler");
                cfg.set("hive.storagehandler.hdfs",
                        "org.apache.hadoop.hive.storagehandlers.hdfs.HDFSStorageHandler");
                cfg.set("hive.storagehandler.ddbs",
                        "org.apache.hadoop.hive.storagehandlers.ddbs.DDBSStorageHandler");
                cfg.set("hive.storagehandler.udw",
                        "org.apache.hadoop.hive.storagehandlers.udw.UDWStorageHandler");
                cfg.set("hive.security.authorization.enabled", "false");
                cfg.set("hive.security.authorization.manager",
                        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
                cfg.set("hive.security.authenticator.manager",
                        "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator");
                cfg.set("hive.security.authorization.createtable.user.grants",
                        "");
                cfg.set("hive.security.authorization.createtable.group.grants",
                        "");
                cfg.set("hive.security.authorization.createtable.role.grants",
                        "");
                cfg.set("hive.security.authorization.createtable.owner.grants",
                        "");
                cfg.set("hive.metastore.checkForDefaultDb", "false");
                if (hadoopConf == null || hadoopConf.size() == 0) {
                    // try to load hadoop-site from HADOOP_HOME
                    String hadoopDir = System.getenv("HADOOP_HOME");
                    log.info("HADOOP_HOME:" + hadoopDir);
                    if (null != hadoopDir) {
                        //run in hadoop 2(baidu internal version)
                        if (new File(hadoopDir + "/conf/hadoop-site.xml").exists()) {
                            cfg.addResource(new Path(hadoopDir + "/conf/hadoop-site.xml"));
                        }
                    }
                }

                if (hiveConf == null || hiveConf.size() == 0) {
                    // try to load hive-site from HIVE_HOME
                    String hiveDir = System.getenv("HIVE_HOME");
                    log.info("HIVE_HOME:" + hiveDir);
                    if (null != hiveDir) {
                        //run in hive-2.x(baidu internal version)
                        if (new File(hiveDir + "/conf/hive-site.xml").exists()) {
                            cfg.addResource(new Path(hiveDir + "/conf/hive-site.xml"));
                        }
                    }
                }
            } else if (hiveType == HHIVE) {
//				cfg.addResource(new Path(DFSUtils.default_hdp_core_config));
//				cfg.addResource(new Path(DFSUtils.default_hdp_hdfs_config));
//				cfg.addResource(new Path(DFSUtils.default_hdp_map_config));
//				cfg.addResource(new Path(DFSUtils.default_hdp_yarn_config));
//
//				cfg.addResource(new Path(hdp_hive_config));
                cfg.set("hive.security.authenticator.manager", "");
                cfg.set("mapred.job.tracker", "remote");
            } else if (hiveType == CHIVE) {
                //do nothing
            }
            cfg.set("hive.metastore.checkForDefaultDb", "false");
            cfg.set("hive.exec.mode.local.auto", "false");
            cfg.set("hive.querylog.location", System.getProperty("user.home") + "/hivelog");
            if (hadoopConf != null && hadoopConf.size() > 0) {
                for (ConfEntry confEntry : hadoopConf) {
                    if (StringUtils.isNotBlank(confEntry.getKey())) {
                        cfg.set(confEntry.getKey(), confEntry.getValue());
                    }
                }
            }

            if (hiveConf != null && hiveConf.size() > 0) {
                for (ConfEntry confEntry : hiveConf) {
                    if (StringUtils.isNotBlank(confEntry.getKey())) {
                        cfg.set(confEntry.getKey(), confEntry.getValue());
                    }
                }
            }
            if (StringUtils.isNotBlank(ugi)) {
                cfg.set("hadoop.job.ugi", ugi);
            }
        }

        log.info("hadoop ugi:" + cfg.get("hadoop.job.ugi"));
        return cfg;
    }

    public static HiveConf getConf(HdfsConf hdfsConf, HiveServer hiveServer, Integer hiveType)
            throws IOException {

        HiveConf cfg = null;
        List<ConfigResource> hadoopResource = hdfsConf.getConfigResources();
        List<ConfigResource> hiveResource = hiveServer.getConfigResources();
        cfg = getConf(hdfsConf.getUgi(), hiveServer.getHiveConf(), hdfsConf.getHadoopConf(), hiveType);

        for (ConfigResource r : hadoopResource) {
            cfg.addResource(new Path(r.getResource()));
        }

        for (ConfigResource r : hiveResource) {
            cfg.addResource(new Path(r.getResource()));
        }

        return cfg;
    }

    public static Map<String, String> calcPartitions(Table t, String partition) {

        Map<String, String> partitionMap = new LinkedHashMap<String, String>();

        if (StringUtils.isBlank(partition)) {
            return partitionMap;
        }

        String[] pArray = partition.split(partition_splitter);

        for (String p : pArray) {
            if (StringUtils.isBlank(p)) {
                continue;
            }
            String[] part = p.trim().split(partition_equal);

            if (part.length == 1) {
                partitionMap.clear();
                List<FieldSchema> fList = t.getPartCols();
                String pName = fList.get(0).getName();
                partitionMap.put(pName, part[0].trim());
                break;
            } else {
                partitionMap.put(part[0].trim(), part[1].trim());
            }
        }
        return partitionMap;
    }
}