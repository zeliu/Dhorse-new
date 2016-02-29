package cn.wanda.dataserv.input.impl;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.core.Line;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.HdfsInputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.DFSUtils;
import cn.wanda.dataserv.utils.charset.CharSetUtils;

@Log4j
public class DfsInput extends AbstractInput implements Input {

//	static{
//		try{
//			System.loadLibrary("hadoop");
//			System.loadLibrary("lzma");
//			System.loadLibrary("lzo2");
//			System.loadLibrary("qlz");
//		}catch(Throwable e){
//			log.warn(e.getMessage(), e);
//		}
//	}

    private static Map<String, Class<? extends DfsFileInputAdapter>> fileAdapterMap;

    static {
        fileAdapterMap = new HashMap<String, Class<? extends DfsFileInputAdapter>>();
        fileAdapterMap.put(DFSUtils.TXT, DfsTextFileInputAdapter.class);
        fileAdapterMap.put(DFSUtils.TXT_COMP,
                DfsCompTextFileInputAdapter.class);
        fileAdapterMap.put(DFSUtils.SEQ,
                DfsSequenceFileInputAdapter.class);
        fileAdapterMap.put(DFSUtils.RCFILE,
                DfsRcFileInputAdapter.class);
    }

    private HdfsInputLocation location;

    private Integer bufferSize = 4 * 1024;

    private Boolean ignoreKey;

    private HdfsConf hdfsConf = new HdfsConf();

    private String ugi;

    private String pathString;

    private Path path;

    private FileSystem fs = null;

    private DfsFileInputAdapter dfsFileAdapter = null;

    private String fieldsSeparator = "\t";

    private String fileType;

    @Override
    public void init() {

        LocationConfig l = this.inputConfig.getLocation();

        if (!HdfsInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException(
                    "input config type is not DFS!");
        }

        location = (HdfsInputLocation) l;

//		DFSUtils.loadHadoopNative(DFSUtils.BHDFS);
        // init hdfs conf
        fieldsSeparator = this.inputConfig.getSchema().getFieldDelim();
        bufferSize = this.location.getBufferSize();
        encoding = this.inputConfig.getEncode();
        ignoreKey = this.location.getIgnoreKey();
        hdfsConf = this.location.getHdfs();
        ugi = this.location.getHdfs().getUgi();
        pathString = ExpressionUtils.getOneValue(this.location.getPath());
        if (StringUtils.isBlank(pathString)) {
            throw new InputException(
                    "can not find param in hdfs input conf: path");
        }

		/* check hdfs file type */
        try {
            /*fs = DFSUtils.createFileSystem(URI.create(pathString),
                    DFSUtils.getConf(pathString, ugi, hdfsConf));*/
        	fs = FileSystem.newInstance(DFSUtils.getConf(pathString, ugi, hdfsConf));
        } catch (Exception e) {
            closeAll();
            throw new InputException(String.format(
                    "Initialize file system failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }

        if (fs == null) {
            closeAll();
            throw new InputException("Create the file system failed.");
        }

        // try to connect dfs
        try {
            fileType = location.getFileType();
            Class<? extends DfsFileInputAdapter> dfsFileAdapterClazz = fileAdapterMap
                    .get(fileType);
            String name = dfsFileAdapterClazz.getName().substring(
                    dfsFileAdapterClazz.getName().lastIndexOf(".") + 1);
            log.info(String.format("input hdfs filetype %s, use %s .", fileType.toString(), name));
            dfsFileAdapter = (DfsFileInputAdapter) dfsFileAdapterClazz
                    .getConstructors()[0].newInstance(this);

        } catch (Exception e) {
            closeAll();
            throw new InputException(String.format(
                    "Initialize file system failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }

        path = new Path(pathString);

        try {
            if (!fs.exists(path)) {
                closeAll();
                throw new InputException("can not find file: " + pathString);
            }

            dfsFileAdapter.open();

        } catch (IOException e) {
            closeAll();
            throw new InputException(String.format(
                    "Initialize file system is failed:%s,%s", e.getMessage(),
                    e.getCause()), e);
        }
    }

    @Override
    public Line readLine() {
        try {
            Line line = dfsFileAdapter.readLine();
            if (line != null) {
                return line;
            } else {
                return Line.EOF;
            }
        } catch (IOException e) {
            throw new InputException(e);
        }

    }

    @Override
    public void close() {
        closeAll();
    }

    private void closeAll() {
        try {
            if (dfsFileAdapter != null) {
                dfsFileAdapter.close();
            }
            IOUtils.closeStream(fs);
        } catch (Exception e) {
            log.warn(String.format(
                    "DfsInput closing failed: %s,%s", e.getMessage(),
                    e.getCause()), e);
        }
    }

    interface DfsFileInputAdapter {
        int open() throws IOException;

        Line readLine() throws IOException;

        void close();
    }

    class DfsSequenceFileInputAdapter implements DfsFileInputAdapter {

        private Configuration conf;

        private SequenceFile.Reader reader = null;

        private Writable key = null;

        private Writable value = null;

        private String keyclass = null;

        private String valueclass = null;

        private boolean isIgnoreKey = false;

        private boolean isIgnoreValue = false;

        public DfsSequenceFileInputAdapter() {
            try {
                this.conf = DFSUtils.getConf(pathString, ugi, hdfsConf);
            } catch (IOException e) {
                throw new InputException(String.format(
                        "Initialize hadoop configure failed:%s,%s", e.getMessage(),
                        e.getCause()), e);
            }
        }

        @Override
        public int open() throws IOException {
            try {
                conf.setInt("io.file.buffer.size", bufferSize);
                reader = new SequenceFile.Reader(fs, path, conf);
                key = (Writable) ReflectionUtils.newInstance(
                        reader.getKeyClass(), conf);
                value = (Writable) ReflectionUtils.newInstance(
                        reader.getValueClass(), conf);
                keyclass = key.getClass().getName();
                valueclass = value.getClass().getName();
                if ((ignoreKey)
                        || ("org.apache.hadoop.io.NullWritable"
                        .equals(keyclass))) {
                    isIgnoreKey = true;
                }
                if ("org.apache.hadoop.io.NullWritable".equals(valueclass)) {
                    isIgnoreValue = true;
                }
                return SUCCESS;
            } catch (EOFException e) {
                log.warn("File is empty file:" + pathString);
                log.debug(e.getMessage(), e);
                return SUCCESS;
            }
        }

        @Override
        public Line readLine() throws IOException {

            Line line = null;
            try {
                if (reader.next(key, value)) {
                    StringBuffer sb = new StringBuffer();
                    if (!isIgnoreKey) {
                        sb.append(key.toString());
                        sb.append(fieldsSeparator);
                    }
                    if (!isIgnoreValue) {
                        sb.append(value.toString());
                    }
                    line = new Line(sb.toString());
                }
            } catch (EOFException e) {
                log.info(e.getMessage(), e);
            }
            return line;

        }

        @Override
        public void close() {
            IOUtils.closeStream(reader);
        }

    }

    class DfsTextFileInputAdapter implements DfsFileInputAdapter {

        private Configuration conf = null;

        private FSDataInputStream in = null;

        private CompressionInputStream cin = null;

        private BufferedReader br = null;

        private boolean compressed = false;

        private DfsTextFileInputAdapter(boolean compressed) {
            this.conf = DFSUtils.newConf();
            this.compressed = compressed;
        }

        public DfsTextFileInputAdapter() {
            this(false);
        }

        @Override
        public int open() throws IOException {
            if (compressed) {
                String codecClassName = location.getCodecClass();

                Class<?> codecClass;
                try {
                    codecClass = Class.forName(codecClassName);
                } catch (ClassNotFoundException e) {
                    throw new OutputException(e);
                }
                Configuration conf = DFSUtils.newConf();
                CompressionCodec codec = (CompressionCodec) ReflectionUtils
                        .newInstance(codecClass, conf);

                if (codec == null) {
                    throw new InputException("can not decompression file:"
                            + path.toString());
                }
                in = fs.open(path);
                cin = codec.createInputStream(in);
                br = new BufferedReader(new InputStreamReader(cin, CharSetUtils.getDecoderForName(encoding)),
                        bufferSize);
            } else {
                in = fs.open(path);
                br = new BufferedReader(new InputStreamReader(in, CharSetUtils.getDecoderForName(encoding)),
                        bufferSize);
            }
            if (in.available() == 0)
                return FAILURE;
            else
                return SUCCESS;
        }

        @Override
        public Line readLine() throws IOException {

            Line line = null;
            try {
                String s = br.readLine();
                if (null != s) {
                    line = new Line(s);
                }
            } catch (EOFException e) {
                log.info(e.getMessage(), e);
            }
            return line;

        }

        @Override
        public void close() {
            IOUtils.cleanup(null, in, cin, br);
        }

    }

    class DfsCompTextFileInputAdapter extends DfsTextFileInputAdapter {
        public DfsCompTextFileInputAdapter() {
            super(true);
        }
    }

    class DfsRcFileInputAdapter implements DfsFileInputAdapter {

        @SuppressWarnings("deprecation")
        private FileSplit split;

        @SuppressWarnings("rawtypes")
        private RCFileRecordReader reader;

        private BytesRefArrayWritable value;

        private LongWritable key;

        private Configuration conf;

        private CharsetDecoder decoder;

        public DfsRcFileInputAdapter() {
            try {
                this.conf = DFSUtils.getConf(pathString, ugi, hdfsConf);
            } catch (IOException e) {
                throw new InputException(String.format(
                        "Initialize hadoop configure failed:%s,%s", e.getMessage(),
                        e.getCause()), e);
            }
        }

        @SuppressWarnings({"deprecation", "rawtypes"})
        @Override
        public int open() throws IOException {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            long fileLen = fs.getFileStatus(path).getLen();

            split = new FileSplit(path, 0, fileLen, new JobConf(conf));
            reader = new RCFileRecordReader(conf, split);
            key = new LongWritable();
            value = new BytesRefArrayWritable();
            decoder = CharSetUtils.getDecoderForName(encoding).
                    onMalformedInput(CodingErrorAction.REPLACE).
                    onUnmappableCharacter(CodingErrorAction.REPLACE);
            return SUCCESS;
        }

        @Override
        public Line readLine() throws IOException {
            Line line = null;
            try {
                if (reader.next(key, value)) {
                    StringBuffer sb = new StringBuffer();
                    getValue(value, sb);
                    line = new Line(sb.toString());
                }
            } catch (EOFException e) {
                log.info(e.getMessage(), e);
            }
            return line;
        }

        @Override
        public void close() {
            try {
                if (reader != null) {
                    reader.close();
                }
                reader = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }

        private void getValue(BytesRefArrayWritable value, StringBuffer buf)
                throws IOException {
            int n = value.size();
            if (n > 0) {
                BytesRefWritable v = value.unCheckedGet(0);
                ByteBuffer bb = ByteBuffer.wrap(v.getData(), v.getStart(),
                        v.getLength());
                CharBuffer vString = decoder.decode(bb);
                if (vString != null) {
                    buf.append(vString);
                }
                for (int i = 1; i < n; i++) {
                    // do not put the TAB for the last column
                    buf.append(fieldsSeparator);

                    v = value.unCheckedGet(i);
                    bb = ByteBuffer.wrap(v.getData(), v.getStart(),
                            v.getLength());
                    vString = decoder.decode(bb);
                    if (vString != null) {
                        buf.append(vString);
                    }
                }
            }
        }

    }

}