package cn.wanda.dataserv.output.impl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.HdfsOutputLocation;
import cn.wanda.dataserv.config.location.HdpOutputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.core.ObjectFactory;
import cn.wanda.dataserv.output.FileExistException;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.DFSUtils;

@Log4j
public class HdpOutput extends DfsOutput {

//	static {
//		try {
//			System.loadLibrary("lzma");
//			System.loadLibrary("lzo2");
//			System.loadLibrary("qlz");
//			System.loadLibrary("hadoop");
//		} catch (Throwable e) {
//			log.debug(e.getMessage(), e);
//			log.warn(e.getMessage());
//		}
//	}

    private static Map<String, Class<? extends DfsFileOutputAdapter>> fileAdapterMap;

    private final static String defaultFileName = "0000001";

    private final static String MANIFEST_FILENAME = "@manifest";

    static {
        fileAdapterMap = new HashMap<String, Class<? extends DfsFileOutputAdapter>>();
        fileAdapterMap.put(DFSUtils.TXT, DfsTextFileOutputAdapter.class);
        fileAdapterMap.put(DFSUtils.TXT_COMP,
                DfsCompTextFileOutputAdapter.class);
        fileAdapterMap.put(DFSUtils.SEQ, DfsSequenceFileOutputAdapter.class);
    }

    private FileSystem fs;

    private Path path = null;

    private Configuration conf;

    /**
     * Line field separator from storage
     */
    private String fieldsSeparator = "\t";

    private String lineSeparator = "\n";

    private int bufferSize = 8 * 1024;

    private String writeType = "1";

    protected int hdftType = DFSUtils.HHDFS;

    private List<ConfEntry> configure = new ArrayList<ConfEntry>();

    private DfsFileOutputAdapter dfsFileAdapter = null;

    private static String[] searchChars = new String[2];

    private HdfsOutputLocation location;

    private boolean genManifest;

    @Override
    public void writeLine(Line line) {
        try {
            dfsFileAdapter.write(line);
        } catch (Exception ex) {
            throw new OutputException(ex);
        }
    }

    @Override
    public void close() {
        closeAll();
    }

    @Override
    public void post(boolean success) {
        if (success && genManifest) {
            this.genManifest();
        }
    }

    @Override
    public void init() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!HdpOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new OutputException("output config type is not HDP!");
        }

//		DFSUtils.loadHadoopNative(DFSUtils.BHDFS);

        location = (HdpOutputLocation) l;

        fieldsSeparator = this.outputConfig.getSchema().getFieldDelim();
        lineSeparator = this.outputConfig.getSchema().getLineDelim();
        searchChars[0] = fieldsSeparator;
        searchChars[1] = lineSeparator;
        bufferSize = this.location.getBufferSize();
        encoding = this.outputConfig.getEncode();
        configure = this.location.getHdfs().getHadoopConf();
        writeType = this.location.getWriteType();

        genManifest = "true".equalsIgnoreCase(this.location.getGenManifest());

        String ugi = this.location.getHdfs().getUgi();
        String pathString = ExpressionUtils.getOneValue(this.location.getPath());
        if (StringUtils.isBlank(pathString)) {
            throw new OutputException(
                    "can not find param in hdfs input conf: path");
        }
        ClassLoader cl = ObjectFactory.dpumpClassLoader.get(HdpOutput.class.getName());
        if (cl != null) {
            Thread.currentThread().setContextClassLoader(cl);
        }

        // get file system
        try {
            conf = DFSUtils.getConf(pathString, ugi, location.getHdfs(), hdftType);
            fs = DFSUtils.createFileSystem(new URI(pathString), conf, hdftType);
        } catch (Exception e) {
            closeAll();
            throw new OutputException(String.format(
                    "Initialize file system failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }

        // get path
        if (pathString != null) {
            if (pathString.endsWith("/")) {
                pathString = pathString + defaultFileName;
            }
            path = new Path(pathString);
        } else {
            closeAll();
            throw new OutputException(
                    "can not find param in hdfs input conf: path");
        }

        try {
            if (fs.exists(path)) {
                if (!fs.isFile(path)) {
                    throw new OutputException("Target path is not a file!");
                }
                if ("1".equals(writeType)) {
                    // delete if exist
                    DFSUtils.deleteFile(fs, path, true);
                } else if ("2".equals(writeType)) {
                    // terminate if exist
                    throw new FileExistException("Target file is Exist");
                }
            }
        } catch (IOException e) {
            closeAll();
            throw new OutputException(String.format(
                    "Initialize DfsOutput failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }
        // get target file type and init file adapter
        try {
            String fileType = this.location.getFileType();

            Class<? extends DfsFileOutputAdapter> dfsFileAdapterClazz = fileAdapterMap
                    .get(fileType);
            String name = dfsFileAdapterClazz.getName().substring(
                    dfsFileAdapterClazz.getName().lastIndexOf(".") + 1);
            log.info(String.format("output hdfs filetype %s, use %s .",
                    fileType, name));
            dfsFileAdapter = (DfsFileOutputAdapter) dfsFileAdapterClazz
                    .getConstructors()[0].newInstance(this);
        } catch (Exception e) {
            closeAll();
            throw new OutputException(String.format(
                    "Initialize hdfs file output adapter failed:%s,%s",
                    e.getMessage(), e.getCause()), e);
        }

        try {

            dfsFileAdapter.open();

        } catch (Exception ex) {
            closeAll();
            throw new OutputException(String.format(
                    "Initialize file system failed:%s,%s", ex.getMessage(),
                    ex.getCause()), ex);
        }

    }

    private void closeAll() {
        try {
            if (dfsFileAdapter != null) {
                dfsFileAdapter.close();
            }
            IOUtils.closeStream(fs);
        } catch (Exception e) {
            log.warn(String.format(
                    "DfsOutput closing failed: %s,%s", e.getMessage(),
                    e.getCause()), e);
        }
    }

    private void genManifest() {
        //TODO
    }

    interface DfsFileOutputAdapter {
        void open();

        void write(Line line);

        void close();
    }

    class DfsSequenceFileOutputAdapter implements DfsFileOutputAdapter {

        private Configuration conf = null;

        private SequenceFile.Writer writer = null;

        private Writable key = null;

        private Writable value = null;

        private boolean compressed = false;

        private String keyClassName = null;

        private String valueClassName = null;

        private Class<?> keyClass = null;

        private Class<?> valueClass = null;

        private Method keySetMethod = null;

        private Method valueSetMethod = null;

        // modified by bazhen.csy
        private int keyFieldIndex = -1;

        private SequenceFile.CompressionType compressioType = SequenceFile.CompressionType.BLOCK;

        public DfsSequenceFileOutputAdapter() {
            String comType = location.getCompressionType();
            if ("BLOCK".equalsIgnoreCase(comType)) {
                compressioType = SequenceFile.CompressionType.BLOCK;
                compressed = true;
            } else if ("RECORD".equalsIgnoreCase(comType)) {
                compressioType = SequenceFile.CompressionType.RECORD;
                compressed = true;
            } else {
                compressioType = SequenceFile.CompressionType.NONE;
            }
            this.conf = DFSUtils.newConf();
        }

        @Override
        public void open() {
            try {

                String codecClassName = location.getCodecClass();
                keyClassName = location.getKeyClass();
                valueClassName = location.getValueClass();

                keyClass = Class.forName(keyClassName);
                valueClass = Class.forName(valueClassName);

                key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
                value = (Writable) ReflectionUtils
                        .newInstance(valueClass, conf);

                keyFieldIndex = location.getKeyFieldIndex();

                if (!keyClassName.toLowerCase().contains("null")
                        && (keyFieldIndex >= 0))
                    keySetMethod = keyClass.getMethod(
                            "set",
                            new Class[]{DFSUtils.getTypeMap().get(
                                    keyClassName)});
                if (!valueClassName.toLowerCase().contains("null"))
                    valueSetMethod = valueClass.getMethod(
                            "set",
                            new Class[]{DFSUtils.getTypeMap().get(
                                    valueClassName)});

                if (compressed) {
                    Class<?> codecClass = Class.forName(codecClassName);
                    CompressionCodec codec = (CompressionCodec) ReflectionUtils
                            .newInstance(codecClass, conf);
                    writer = SequenceFile.createWriter(fs, conf, path,
                            keyClass, valueClass, compressioType, codec);
                } else {
                    writer = SequenceFile.createWriter(fs, conf, path,
                            keyClass, valueClass);
                }
            } catch (Exception e) {
                throw new OutputException(e);
            }
        }

        @Override
        public void write(Line line) {
            try {

                StringBuilder sb = new StringBuilder(10240);
                if (keyFieldIndex >= 0) {
                    String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line.getLine(), fieldsSeparator);

                    if (keySetMethod != null)
                        keySetMethod.invoke(
                                key,
                                new Object[]{adapterType(
                                        fields[keyFieldIndex], keyClassName)});
                    for (int i = 0; i < fields.length; i++) {

                        if (i != keyFieldIndex) {
                            sb.append(fields[i]);
                            sb.append(fieldsSeparator);
                        }
                    }
                    sb.delete(sb.length() - fieldsSeparator.length(),
                            sb.length());
                } else {
                    sb.append(line.getLine());
                }
                if (valueSetMethod != null)
                    valueSetMethod.invoke(
                            value,
                            new Object[]{adapterType(sb.toString(),
                                    valueClassName)});
                writer.append(key, value);
                sb.setLength(0);
            } catch (Exception e) {
                throw new OutputException(e);
            }

        }

        private Object adapterType(String field, String typename) {
            Object target = null;
            if (typename.toLowerCase().contains("null")) {
                target = null;
            } else if (typename.toLowerCase().contains("text")) {
                target = field;
            } else if (typename.toLowerCase().contains("long")) {
                target = Long.parseLong(field);
            } else if (typename.toLowerCase().contains("integer")) {
                target = Integer.parseInt(field);
            } else if (typename.toLowerCase().contains("double")) {
                target = Double.parseDouble(field);
            } else if (typename.toLowerCase().contains("float")) {
                target = Float.parseFloat(field);
            } else {
                target = field;
            }
            return target;
        }

        @Override
        public void close() {
            IOUtils.closeStream(writer);
        }

    }

    class DfsTextFileOutputAdapter implements DfsFileOutputAdapter {

        private FSDataOutputStream out = null;

        private BufferedWriter bw = null;

        private CompressionOutputStream co = null;

        private boolean compressed = false;

        private DfsTextFileOutputAdapter(boolean compressed) {
            super();
            this.compressed = compressed;
        }

        public DfsTextFileOutputAdapter() {
            this(false);
        }

        @Override
        public void open() {
            try {
                boolean flag = true;
                if (compressed) {
                    String codecClassName = location.getCodecClass();

                    Class<?> codecClass = Class.forName(codecClassName);
                    Configuration conf = DFSUtils.newConf();
                    CompressionCodec codec = (CompressionCodec) ReflectionUtils
                            .newInstance(codecClass, conf);

                    out = fs.create(path, flag, bufferSize);
                    co = codec.createOutputStream(out);
                    bw = new BufferedWriter(
                            new OutputStreamWriter(co, encoding), bufferSize);
                } else {
                    out = fs.create(path, flag, bufferSize);
                    bw = new BufferedWriter(new OutputStreamWriter(out,
                            encoding), bufferSize);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new OutputException(e);
            }
        }

        @Override
        public void write(Line line) {
            try {
                bw.write(line.getLine());
                bw.write(lineSeparator);
                bw.flush();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new OutputException(e);
            }
        }

        @Override
        public void close() {
            IOUtils.cleanup(null, bw, out, co);
        }

    }

    class DfsCompTextFileOutputAdapter extends DfsTextFileOutputAdapter {

        public DfsCompTextFileOutputAdapter() {
            super(true);
        }
    }

}