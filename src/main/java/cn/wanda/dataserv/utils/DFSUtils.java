package cn.wanda.dataserv.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import cn.wanda.dataserv.config.location.ConfEntry;
import cn.wanda.dataserv.config.location.ConfigResource;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.utils.hdfs.FileType;

@Log4j
public class DFSUtils {

    public static final String TXT = "txt";
    public static final String SEQ = "seq";
    public static final String RCFILE = "rcfile";
    public static final String TXT_COMP = "txt_comp";

    public static final Integer HHDFS = 1;
    public static final Integer CHDFS = 3;

    //codecClass
    public static final String TableCodec = "none";
    public static final String DefaultCodec = "org.apache.hadoop.io.compress.DefaultCodec";
    public static final String GzipCodec = "org.apache.hadoop.io.compress.GzipCodec";
    public static final String BZip2Codec = "org.apache.hadoop.io.compress.BZip2Codec";
    public static final String LzopCodec = "org.apache.hadoop.io.compress.LzopCodec";
    public static final String LzoCodec = "org.apache.hadoop.io.compress.LzoCodec";
    public static final String LzmaCodec = "org.apache.hadoop.io.compress.LzmaCodec";
    public static final String QuickLzCodec = "org.apache.hadoop.io.compress.QuickLzCodec";

    public final static String default_hadoop_config = "config/hadoop-default.xml";
    public final static String default_hdp_core_config = "config/core-default.xml";
    public final static String default_hdp_map_config = "config/mapred-default.xml";
    public final static String default_hdp_yarn_config = "config/yarn-default.xml";
    public final static String default_hdp_hdfs_config = "config/hdfs-default.xml";

    private static Map<String, Class<?>> typeMap = null;

    static {
        typeMap = new HashMap<String, Class<?>>();
        typeMap.put("org.apache.hadoop.io.BooleanWritable", boolean.class);
        typeMap.put("org.apache.hadoop.io.ByteWritable", byte.class);
        typeMap.put("org.apache.hadoop.io.IntWritable", int.class);
        typeMap.put("org.apache.hadoop.io.VIntWritable", int.class);
        typeMap.put("org.apache.hadoop.io.LongWritable", long.class);
        typeMap.put("org.apache.hadoop.io.VLongWritable", long.class);
        typeMap.put("org.apache.hadoop.io.DoubleWritable", double.class);
        typeMap.put("org.apache.hadoop.io.FloatWritable", float.class);
        typeMap.put("org.apache.hadoop.io.Text", String.class);
    }

    private DFSUtils() {
    }

    public static Map<String, Class<?>> getTypeMap() {
        return typeMap;
    }

    // store configurations for per FileSystem schema
    private static Hashtable<String, Configuration> confs = new Hashtable<String, Configuration>();

    public static Configuration getConf(String dir, String ugi, HdfsConf hdfsConf)
            throws IOException {
        return getConf(dir, ugi, hdfsConf, HHDFS);
    }

    public static void loadHadoopNative(Integer hdfsType) {

        String installDir = "";
        if (System.getenv().get("DPUMP_HOME") != null) {
            installDir = System.getenv().get("DPUMP_HOME");
        } else {
            installDir = "..";
        }

        if (hdfsType == HHDFS) {
            String libpath = null;
            if (new File(installDir + "/pluginlib/libhadoop.so").exists()) {
                libpath = installDir + "/pluginlib/libhadoop.so";
            } else if (System.getenv("HADOOP_HOME") != null) {
                libpath = System.getenv().get("HADOOP_HOME") + "/lib/native/libhadoop.so";
            }
            if (libpath != null) {
                try {
                    System.load(libpath);
                    //System.loadLibrary("hadoop");
                    //System.loadLibrary("lzma");
                    //System.loadLibrary("lzo2");
                    //System.loadLibrary("qlz");
                } catch (Throwable e) {
                    log.warn(e.getMessage(), e);
                }
            }
        }
    }


    /**
     * Get {@link Configuration}.
     *
     * @param dir      directory path in hdfs
     * @param ugi      hadoop ugi
     * @param hdfsConf
     * @return {@link Configuration}
     * @throws IOException
     */

    public static Configuration getConf(String dir, String ugi, HdfsConf hdfsConf, Integer hdfsType)
            throws IOException {

        List<ConfEntry> conf = hdfsConf.getHadoopConf();
        List<ConfigResource> configResources = hdfsConf.getConfigResources();
        URI uri = null;
        Configuration cfg = null;
        String scheme = null;
        try {
            uri = new URI(dir);
            scheme = uri.getScheme();
            if (null == scheme) {
                throw new IOException("HDFS Path missing scheme, check path begin with hdfs://ip:port/  or hdfs://${clustername}");
            }
            cfg = confs.get(scheme);
        } catch (URISyntaxException e) {
            throw new IOException(e.getMessage(), e.getCause());
        }


        if (cfg == null) {
            cfg = new Configuration();
            cfg.setClassLoader(DFSUtils.class.getClassLoader());

            if (configResources != null) {
                log.debug("config resource size is: " + configResources.size());
                for (ConfigResource resource : configResources) {
                    cfg.addResource(new Path(resource.getResource()));
                    log.debug("add resource:" + resource.getResource());
                }
            }

            if (uri.getScheme() != null) {
                String fsname = String.format("%s://%s", uri.getScheme(), uri.getHost());
                if (uri.getPort() != -1) {
                    fsname += ":" + uri.getPort();
                }
                log.info("fs.default.name=" + fsname);
                cfg.set("fs.defaultFS", fsname);
            }

            if (conf != null && conf.size() > 0) {
                for (ConfEntry confEntry : conf) {
                    if (StringUtils.isNotBlank(confEntry.getKey())) {
                        log.debug("key: " + confEntry.getKey() + ",value:" + confEntry.getValue());
                        cfg.set(confEntry.getKey(), confEntry.getValue());
                    }
                }
            }

            if (StringUtils.isNotBlank(ugi)) {
                cfg.set("hadoop.job.ugi", ugi);
            }
            confs.put(scheme, cfg);
        }

        return cfg;
    }

    /**
     * Get one handle of {@link FileSystem}.
     *
     * @param dir      directory path in hdfs
     * @param ugi      hadoop ugi
     * @param hdfsConf hadoop-site.xml path
     * @return one handle of {@link FileSystem}.
     * @throws IOException
     */

    public static FileSystem getFileSystem(String dir, String ugi,
                                           HdfsConf hdfsConf) throws IOException {
        return FileSystem.get(getConf(dir, ugi, hdfsConf));
    }

    public static Configuration newConf() {
        Configuration conf = new Configuration();
        /*
		 * it's weird, we need jarloader as the configuration's classloader but,
		 * I don't know what does the fucking code means Why they need the
		 * fucking currentThread ClassLoader If you know it, Pls add comment
		 * below.
		 * 
		 * private ClassLoader classLoader; { classLoader =
		 * Thread.currentThread().getContextClassLoader(); if (classLoader ==
		 * null) { classLoader = Configuration.class.getClassLoader(); } }
		 */
        conf.setClassLoader(DFSUtils.class.getClassLoader());

        return conf;
    }

    /**
     * Delete file specified by path or files in directory specified by path.
     *
     * @param dfs    handle of {@link FileSystem}
     * @param path   {@link Path} in hadoop
     * @param flag   need to do delete recursively
     * @param isGlob need to use file pattern to match all files.
     * @throws IOException
     */
    public static void deleteFiles(FileSystem dfs, Path path, boolean flag,
                                   boolean isGlob) throws IOException {
        List<Path> paths = listDir(dfs, path, isGlob);
        for (Path p : paths) {
            deleteFile(dfs, p, flag);
        }
    }

    /**
     * List the statuses of the files/directories in the given path if the path
     * is a directory.
     *
     * @param dfs     handle of {@link FileSystem}
     * @param srcpath Path in {@link FileSystem}
     * @param isGlob  need to use file pattern
     * @return all {@link Path} in srcpath
     * @throws IOException
     */
    public static List<Path> listDir(FileSystem dfs, Path srcpath,
                                     boolean isGlob) throws IOException {
        List<Path> list = new ArrayList<Path>();
        FileStatus[] status = null;
        if (isGlob) {
            status = dfs.globStatus(srcpath);
        } else {
            status = dfs.listStatus(srcpath);
        }
        if (status != null) {
            for (FileStatus state : status) {
                list.add(state.getPath());
            }
        }

        return list;
    }

    /**
     * Delete file specified by path.
     *
     * @param dfs  handle of {@link FileSystem}
     * @param path {@link Path} in hadoop
     * @param flag need to do delete recursively
     * @throws IOException
     */
    public static void deleteFile(FileSystem dfs, Path path, boolean flag) throws IOException {
        log.debug("deleting:" + path.getName());
        dfs.delete(path, flag);
    }

    /**
     * Initialize handle of {@link FileSystem}.
     *
     * @param uri  URI
     * @param conf {@link Configuration}
     * @return an FileSystem instance
     */

    public static FileSystem createFileSystem(URI uri, Configuration conf)
            throws IOException {
        return createFileSystem(uri, conf, HHDFS);
    }

    public static FileSystem createFileSystem(URI uri, Configuration conf, Integer hdfsType)
            throws IOException {
        FileSystem fs = new Path(uri).getFileSystem(conf);
        fs.initialize(uri, conf);
        return fs;
    }

    /**
     * Check file type in hdfs.
     *
     * @param fs
     *            handle of {@link FileSystem}
     *
     * @param path
     *            hdfs {@link Path}
     *
     * @param conf
     *            {@link Configuration}
     *
     * @return {@link HdfsFileType} TXT, TXT_COMP, SEQ
     * */
//	public static String checkFileType(FileSystem fs, Path path,
//			Configuration conf) throws IOException {
//		FSDataInputStream is = null;
//		try {
//			is = fs.open(path);
//			/* file is empty, use TXT readerup */
//			if (0 == is.available()) {
//				return TXT;
//			}
//			byte[] versionBlock = new byte[27];
//			is.readFully(versionBlock);
//			switch (versionBlock[0]) {
//			case 0x53:
//				char[] blockEnd = {(char) versionBlock[23], (char) versionBlock[24],
//						(char) versionBlock[25], (char) versionBlock[26]};
//				if ("hive".equals(new String(blockEnd))) {
//					return RCFILE;
//				}else{
//					return SEQ;
//				}
//			default:
//				is.seek(0);
//				CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(
//						conf);
//				CompressionCodec codec = compressionCodecFactory.getCodec(path);
//				if (null == codec)
//					return TXT;
//				else {
//					return TXT_COMP;
//				}
//			}
//		} catch (IOException e) {
//			throw e;
//		} finally {
//			if (null != is)
//				is.close();
//		}
//	}

    /**
     * @param path hdfs path
     * @param c    {@link Configuration}
     * @return if given path is file then return itself,<br>
     * if given path is directory then return all <b>files</b> in given path
     * @throws IOException
     * @throws URISyntaxException
     */
    public static List<String> getPathList(String path, Configuration c) throws IOException, URISyntaxException {
        FileSystem fs = DFSUtils.createFileSystem(new URI(path), c);
        List<Path> pathList = listDir(fs, new Path(path), false);
        List<String> result = new ArrayList<String>();
        for (Path p : pathList) {
            if (fs.isFile(p)) {
                result.add(p.toString());
            }else {
            	result.addAll(getPathList(p.toString(), c));
            }
        }
        return result;
    }

    /**
     * @return true if p is a SequenceFile, or a directory containing
     * SequenceFiles.
     */
    public static boolean isSequenceFiles(Configuration conf, Path p)
            throws IOException {
        return getFileType(conf, p) == FileType.SEQUENCE_FILE;
    }

    /**
     * @return the type of the file represented by p (or the files in p, if a
     * directory)
     */
    public static FileType getFileType(Configuration conf, Path p)
            throws IOException {
        FileSystem fs = p.getFileSystem(conf);

        try {
            FileStatus stat = fs.getFileStatus(p);

            if (null == stat) {
                // Couldn't get the item.
                log.warn("Input path " + p + " does not exist");
                return FileType.UNKNOWN;
            }

            if (stat.isDir()) {
                FileStatus[] subitems = fs.listStatus(p);
                if (subitems == null || subitems.length == 0) {
                    log.warn("Input path " + p + " contains no files");
                    return FileType.UNKNOWN; // empty dir.
                }

                // Pick a child entry to examine instead.
                boolean foundChild = false;
                for (int i = 0; i < subitems.length; i++) {
                    stat = subitems[i];
                    if (!stat.isDir()
                            && !stat.getPath().getName().startsWith("_")) {
                        foundChild = true;
                        break; // This item is a visible file. Check it.
                    }
                }

                if (!foundChild) {
                    stat = null; // Couldn't find a reasonable candidate.
                }
            }

            if (null == stat) {
                log.warn("null FileStatus object in isSequenceFiles(); "
                        + "assuming false.");
                return FileType.UNKNOWN;
            }

            Path target = stat.getPath();

            return fromMagicNumber(target, conf);
        } catch (FileNotFoundException fnfe) {
            log.warn("Input path " + p + " does not exist");
            return FileType.UNKNOWN; // doesn't exist!
        }
    }

    /**
     * @param file a file to test.
     * @return true if 'file' refers to a SequenceFile.
     */
    private static FileType fromMagicNumber(Path file, Configuration conf) {
        // Test target's header to see if it contains magic numbers indicating
        // its
        // file type
        byte[] versionBlock = new byte[27];
        FSDataInputStream is = null;
        try {
            FileSystem fs = file.getFileSystem(conf);
            is = fs.open(file);
			
			/* file is empty, use TXT readerup */
            if (0 == is.available()) {
                return FileType.TEXT;
            }

            is.readFully(versionBlock);
        } catch (IOException ioe) {
            // Error reading header or EOF; assume unknown
            log.warn("IOException checking input file header: " + ioe);
            return FileType.UNKNOWN;
        } finally {
            try {
                if (null != is) {
                    is.close();
                }
            } catch (IOException ioe) {
                // ignore; closing.
                log.warn("IOException closing input stream: " + ioe
                        + "; ignoring.");
            }
        }
        if (versionBlock[0] == 'S' && versionBlock[1] == 'E' && versionBlock[2] == 'Q') {
            char[] blockEnd = {(char) versionBlock[23], (char) versionBlock[24],
                    (char) versionBlock[25], (char) versionBlock[26]};
            if ("hive".equals(new String(blockEnd))) {
                return FileType.RCFILE;
            } else {
                return FileType.SEQUENCE_FILE;
            }
        }
        if (versionBlock[0] == 'O' && versionBlock[1] == 'b' && versionBlock[2] == 'j') {
            return FileType.AVRO_DATA_FILE;
        }
        return FileType.UNKNOWN;
    }

    public static String getDefaultExtension(String codecClass) {
        Class<?> cls;
        try {
            cls = Thread.currentThread().getContextClassLoader().loadClass(codecClass);
            if (CompressionCodec.class.isAssignableFrom(cls)) {
                CompressionCodec codec = cls.asSubclass(CompressionCodec.class).newInstance();
                return codec.getDefaultExtension();
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        String path = "hdfs://dataserv/user/hadoop/test";
        URI uri = new URI(path);
        if (uri.getScheme() != null) {
            String fsname = String.format("%s://%s:%s", uri.getScheme(),
                    uri.getHost(), uri.getPort());
            System.out.println(fsname);
        }
    }
}
