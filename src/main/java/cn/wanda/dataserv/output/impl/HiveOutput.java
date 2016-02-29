package cn.wanda.dataserv.output.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.HiveOutputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.DFSUtils;
import cn.wanda.dataserv.utils.HiveUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;


@SuppressWarnings("deprecation")
@Log4j
public class HiveOutput extends AbstractOutput {

    private static Map<Class<? extends HiveOutputFormat>, Class<? extends HiveOutputFormatAdapter>> hiveAdapterMap;

    private final static String urisplit = "/";
    private final static String hivedefaultfilename = "0000001";
    private final static String hivetempfilename = "_dpumptemp/00001";

    private static final Pattern pattern = Pattern.compile("(?<=\t|^)(?=\t|$)");

    private static final String hive_load_null = "\\\\N";

    static {
        hiveAdapterMap = new HashMap<Class<? extends HiveOutputFormat>, Class<? extends HiveOutputFormatAdapter>>();
        hiveAdapterMap.put(RCFileOutputFormat.class,
                RCFileOutputFormatAdapter.class);
        hiveAdapterMap.put(HiveIgnoreKeyTextOutputFormat.class,
                TextFileOutputFormatAdapter.class);
        hiveAdapterMap.put(HiveSequenceFileOutputFormat.class,
                SeqOutputFormatAdapter.class);

//		try{
//			System.loadLibrary("hadoop");
//			System.loadLibrary("lzma");
//			System.loadLibrary("lzo2");
//			System.loadLibrary("qlz");
//		}catch(Throwable e){
//			log.warn(e.getMessage(), e);
//		}
    }

    private HiveOutputLocation location;

    private String tableName;

    private String dbName;

    private String partition;

    private String codecClass = DFSUtils.TableCodec;

    private List<ConfEntry> hadoopSite = new ArrayList<ConfEntry>();

    private List<ConfEntry> hiveSite = new ArrayList<ConfEntry>();

    private String ugi;

    private String fieldDelim = "\t";

    private int bufferSize = 8 * 1024;

    // ///////////////////////////

    private HiveConf conf;

    private Hive hive;
    private Table t;

    private HiveOutputFormatAdapter adapter;

    protected String pathString;

    protected Map<String, String> partitionMap;


    @Override
    public void writeLine(Line line) {
        try {
            adapter.write(line);
        } catch (Exception ex) {
            throw new OutputException(ex);
        }
    }

    @Override
    public void close() {
        if (adapter != null) {
            adapter.close();
        }
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void init() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!HiveOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new OutputException("output config type is not Hive!");
        }

        DFSUtils.loadHadoopNative(DFSUtils.HHDFS);

        location = (HiveOutputLocation) l;

//		fieldDelim = this.outputConfig.getSchema().getFieldDelim();
        bufferSize = this.location.getBufferSize();
        encoding = this.outputConfig.getEncode();
        hadoopSite = this.location.getHdfs().getHadoopConf();
        hiveSite = this.location.getHive().getHiveConf();
        tableName = ExpressionUtils.getOneValue(this.location.getTableName());
        if (StringUtils.isBlank(tableName)) {
            throw new OutputException("table name is empty.");
        }

        dbName = this.location.getDbName();
        partition = ExpressionUtils.getOneValue(this.location.getPartition());
        ugi = this.location.getHdfs().getUgi();

        codecClass = this.location.getCodecClass();

        // get hive conf
        try {
            conf = HiveUtils.getConf(ugi, hiveSite, hadoopSite);
        } catch (IOException e) {
            throw new OutputException(String.format(
                    "Initialize hive conf failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }

        // get hive instance and table
        try {
            hive = Hive.get(conf);
            t = hive.getTable(dbName, tableName, true);
        } catch (HiveException e) {
            throw new OutputException(String.format(
                    "Initialize hive conf failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }
        // get hiveoutputformat class
        Class<? extends OutputFormat> ofClazz = t.getOutputFormatClass();

        Class<? extends HiveOutputFormatAdapter> adapterClass = hiveAdapterMap
                .get(ofClazz);

        // get adapter instance and init it
        try {
            this.adapter = (HiveOutputFormatAdapter) adapterClass.getConstructors()[0].newInstance(this);
            HiveOutputFormat hof = (HiveOutputFormat)ofClazz.newInstance();
            this.adapter.open(hof);
        } catch (Exception e) {
            throw new OutputException(String.format(
                    "Initialize hive adapter failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }

    }

    @Override
    public void post(boolean success) {
        // do init
        try {
            location = (HiveOutputLocation) this.outputConfig.getLocation();
            hadoopSite = this.location.getHdfs().getHadoopConf();
            hiveSite = this.location.getHive().getHiveConf();
            ugi = this.location.getHdfs().getUgi();
            tableName = ExpressionUtils.getOneValue(this.location.getTableName());
            dbName = this.location.getDbName();
            partition = ExpressionUtils.getOneValue(this.location.getPartition());

            codecClass = this.location.getCodecClass();

            conf = HiveUtils.getConf(ugi, hiveSite, hadoopSite);
            hive = Hive.get(conf);
            t = hive.getTable(dbName, tableName, true);
            partitionMap = HiveUtils.calcPartitions(t, partition);
            if (success) {
                //update hive partition info
                //update partition if table is a partitioned table

                List<String> parVal = new ArrayList<String>();
                for (String val : partitionMap.values()) {
                    parVal.add(val);
                }
                try {
                    // drop partition if table is a partitioned table
                    // drop if exist
                    if (parVal.size() > 0) {
                        hive.dropPartition(dbName, tableName, parVal, true);
                    } else {
                        //
                        hive.dropTable(dbName, tableName, true, false);
                        hive.createTable(t, true);
                    }
                } catch (HiveException e) {
                    // Partition or table doesn't exist
                    log.warn(e.toString());
                    log.debug(e.getMessage(), e);
                }
                Path path = null;
                Path tempPath = null;
                if (partitionMap.size() > 0) {
                    URI tURI = t.getDataLocation().toUri();
                    Path partPath = new Path(tURI.getPath(),
                            Warehouse.makePartPath(partitionMap));
                    Path newPartPath = new Path(tURI.getScheme(), tURI.getAuthority(),
                            partPath.toUri().getPath());
                    String hiveFileName = hivedefaultfilename;
                    String extension = DFSUtils.getDefaultExtension(codecClass);
                    if (StringUtils.isNotBlank(extension)) {
                        hiveFileName = hiveFileName + extension;
                    }
                    path = new Path(newPartPath.toString() + urisplit + hiveFileName);
                    tempPath = new Path(newPartPath.toString() + urisplit + hivetempfilename);
                    pathString = path.toString();
                    //new partition if table is a partitioned table
                    hive.getPartition(t, partitionMap, true, pathString, true);
                } else {
                    URI tURI = t.getDataLocation().toUri();
                    path = new Path(tURI.getPath());
                    tempPath = new Path(tURI.getPath() + hivetempfilename);
                    pathString = path.toString();
                }

                // move data
                path.getFileSystem(conf).rename(tempPath, path);
                if (path.getFileSystem(conf).exists(tempPath.getParent())) {
                    path.getFileSystem(conf).delete(tempPath.getParent(), true);
                }
                if (partitionMap.size() > 0) {
                    hive.getPartition(t, partitionMap, true, pathString, true);
                }

            } else {
                Path tempPath = null;
                if (partitionMap.size() > 0) {
                    URI tURI = t.getDataLocation().toUri();
                    Path partPath = new Path(tURI.getPath(),
                            Warehouse.makePartPath(partitionMap));
                    Path newPartPath = new Path(tURI.getScheme(), tURI.getAuthority(),
                            partPath.toUri().getPath());
                    tempPath = new Path(newPartPath.toString() + urisplit + hivetempfilename);
                } else {
                    URI tURI = t.getDataLocation().toUri();
                    tempPath = new Path(tURI.getPath() + hivetempfilename);
                }
                DFSUtils.deleteFile(tempPath.getFileSystem(conf), tempPath, true);
            }
        } catch (HiveException e) {
            // Partition or table doesn't exist
            log.warn(e.toString());
            log.debug(e.getMessage(), e);
        } catch (MetaException e) {
            throw new OutputException("HiveOutput write hive metadata failed.", e);
        } catch (IOException e) {
            throw new OutputException("HiveOutput get filesystem failed.", e);
        }
    }

    interface HiveOutputFormatAdapter {

        void open(HiveOutputFormat hof);

        void write(Line line);

        void close();
    }

    abstract class AbstractHiveOutputFormatAdapter implements
            HiveOutputFormatAdapter {

        protected RecordWriter rw;

        protected String tFieldDelim;
        protected String tLineDelim;

        protected Path path = null;

        protected SerDe serde;

        public AbstractHiveOutputFormatAdapter() {

        }

        @Override
        public void open(HiveOutputFormat hof) {
            try {

                Serializer serializer = (Serializer) t.getDeserializer();
                Class<? extends Writable> outputClass = serializer
                        .getSerializedClass();

                // get job conf
                JobConf jc = new JobConf(conf);

                // get table field delim and line delim
                Map<String, String> serdeInfoParamMap = t.getTTable().getSd()
                        .getSerdeInfo().getParameters();
                tFieldDelim = serdeInfoParamMap.get("field.delim");
                tLineDelim = serdeInfoParamMap.get("line.delim");

                // get table location
                partitionMap = HiveUtils.calcPartitions(t, partition);

                if (partitionMap.size() > 0) {
                    URI tURI = t.getDataLocation().toUri();
                    Path partPath = new Path(tURI.getPath(),
                            Warehouse.makePartPath(partitionMap));
                    Path newPartPath = new Path(tURI.getScheme(), tURI.getAuthority(),
                            partPath.toUri().getPath());
                    path = new Path(newPartPath.toString() + urisplit + hivetempfilename);
                } else {
                    URI tURI = t.getDataLocation().toUri();
                    path = new Path(tURI.getPath() + hivetempfilename);
                }

                if (DFSUtils.TableCodec.equalsIgnoreCase(codecClass)) {
                    rw = hof.getHiveRecordWriter(jc, path, outputClass, t
                                    .getTTable().getSd().isCompressed(), t.getMetadata(),
                            null);
                } else {
                    jc.set("mapred.output.compression.codec", codecClass);
                    jc.set("hive.exec.compress.output", "true");
                    jc.set("mapreduce.output.fileoutputformat.compress.codec", codecClass);
                    rw = hof.getHiveRecordWriter(jc, path, outputClass, true, t.getMetadata(),
                            null);
                }
                serde = (SerDe) t.getDeserializer();

            } catch (Exception e) {
                throw new OutputException("HiveOutput get RecordWrite failed.",
                        e);
            }
        }

        @Override
        public void close() {
            try {

                if (rw != null) {
                    rw.close(true);
                }
            } catch (IOException e) {
                log.warn("HiveOutput close failed.", e);
            }

        }

    }

    class RCFileOutputFormatAdapter extends AbstractHiveOutputFormatAdapter
            implements HiveOutputFormatAdapter {

        public RCFileOutputFormatAdapter() {
            super();
        }

        @Override
        public void write(Line line) {
            try {
                String lStr = pattern.matcher(line.getLine()).replaceAll(hive_load_null);
                byte[][] record = this.getByteArray(lStr);
                BytesRefArrayWritable bytes = new BytesRefArrayWritable(
                        record.length);
                for (int i = 0; i < record.length; i++) {
                    BytesRefWritable cu = new BytesRefWritable(record[i], 0,
                            record[i].length);
                    bytes.set(i, cu);
                }
                rw.write(bytes);
                bytes.clear();
            } catch (Exception e) {
                throw new OutputException("HiveOutput write failed.", e);
            }
        }

        private byte[][] getByteArray(String target)
                throws UnsupportedEncodingException {
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(target, fieldDelim);
            byte[][] result = new byte[fields.length][];

            for (int i = 0; i < fields.length; i++) {
                result[i] = fields[i].getBytes(encoding);
            }

            return result;
        }
    }

    class TextFileOutputFormatAdapter extends AbstractHiveOutputFormatAdapter
            implements HiveOutputFormatAdapter {
        private Text text;

        private ObjectInspector oi;

        private String tableFieldDelim;

        public TextFileOutputFormatAdapter() {
            super();
        }

        @Override
        public void open(HiveOutputFormat hof) {
            super.open(hof);

            tableFieldDelim = t.getSerdeParam("field.delim");

            if (StringUtils.isBlank(tableFieldDelim)) {
                tableFieldDelim = "\t";
            }
            text = new Text();
            try {
                oi = serde.getObjectInspector();
            } catch (SerDeException e) {
                throw new OutputException(String.format(
                        "Initialize hive adapter failed:%s,%s", e.getMessage(),
                        e.getCause()), e);
            }
        }

        @Override
        public void write(Line line) {

            String lStr = pattern.matcher(line.getLine()).replaceAll(hive_load_null);
            String lineToWrite = converFieldDelim(lStr);
            text.set(lineToWrite);
            Object row;
            try {
                row = serde.deserialize(text);

                Writable outVal = serde.serialize(row, oi);
                rw.write(outVal);
                text.clear();
            } catch (SerDeException e) {
                throw new OutputException("HiveOutput write failed.", e);
            } catch (IOException e) {
                throw new OutputException("HiveOutput write failed.", e);
            }
        }

        private String converFieldDelim(String line) {
            return StringUtils.replace(line, fieldDelim, tableFieldDelim);
        }
    }

    class SeqOutputFormatAdapter extends AbstractHiveOutputFormatAdapter
            implements HiveOutputFormatAdapter {

        private Text t;

        private ObjectInspector oi;

        public SeqOutputFormatAdapter() {
            super();
        }

        @Override
        public void open(HiveOutputFormat hof) {
            super.open(hof);

            t = new Text();
            try {
                oi = serde.getObjectInspector();
            } catch (SerDeException e) {
                throw new OutputException(String.format(
                        "Initialize hive adapter failed:%s,%s", e.getMessage(),
                        e.getCause()), e);
            }
        }

        @Override
        public void write(Line line) {

            String lStr = pattern.matcher(line.getLine()).replaceAll(hive_load_null);
            t.set(lStr);
            Object row;
            try {
                row = serde.deserialize(t);

                Writable outVal = serde.serialize(row, oi);
                rw.write(outVal);
                t.clear();
            } catch (SerDeException e) {
                throw new OutputException("HiveOutput write failed.", e);
            } catch (IOException e) {
                throw new OutputException("HiveOutput write failed.", e);
            }
        }

    }

}