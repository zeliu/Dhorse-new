package cn.wanda.dataserv.output.impl;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.MysqlOutputLocation;
import cn.wanda.dataserv.config.resource.MysqlServer;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.engine.OutputWorker;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.DBSource;
import com.mysql.jdbc.Statement;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@Log4j
public class OracleOutput extends AbstractOutput implements Output {

    private static String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

    private static Map<String, String> encodingMaps = null;
    private static List<String> encodingConfigs = null;

    private final static char lineDelim = '\n';

    private static final Pattern pattern = Pattern.compile("(?<=\t|^)(?=\t|$)");

    private static final String mysql_load_null = "\\\\N";

    static {
        encodingMaps = new HashMap<String, String>();
        encodingMaps.put("UTF-8", "UTF8");
        encodingMaps.put("utf-8", "UTF8");

        encodingConfigs = new ArrayList<String>();
        encodingConfigs.add("character_set_client");
        encodingConfigs.add("character_set_connection");
        encodingConfigs.add("character_set_database");
        encodingConfigs.add("character_set_results");
        encodingConfigs.add("character_set_server");
    }

    // input params
    private MysqlOutputLocation location;

    private String user = "";

    private String passwd = "";

    private String host = "";

    private String port = "1521";

    private String db = null;

    private String params;

    private String tbName;

    private String fieldDelim = "\t";

    private String set;

    private String replace;

    private int concurrency;

    //

    private PipedOutputStream pos;

    private Connection connection;

    private Statement stmt = null;

    private String loadSql;

    private ExecutorService e;

    @Override
    public void writeLine(Line line) {

        try {
            pos.write(pattern.matcher(line.getLine()).replaceAll(mysql_load_null).getBytes(this.encoding));
            pos.write(lineDelim);
            pos.flush();
        } catch (IOException e) {
            throw new OutputException("MysqlOutput write failed.", e);
        }
    }

    @Override
    public void close() {
        if (null != pos) {
            try {
                pos.close();
            } catch (IOException e) {
                log.warn("Mysql output close failed!", e);
            }
        }

        e.shutdown();
        while (!e.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Mysql output close failed!", e);
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Mysql output close failed!", e);
            }
        }
        log.info("Mysql output finished!");
    }

    @Override
    public void prepare() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!MysqlOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not Hive!");
        }

        this.location = (MysqlOutputLocation) l;

        if (StringUtils.isBlank(this.location.getPreSql())) {
            return;
        }

        this.encoding = this.outputConfig.getEncode();

        this.concurrency = this.runtimeConfig.getOutputConcurrency();

        MysqlServer mysqlServer = this.location.getMysql();

        this.user = mysqlServer.getUser();

        this.passwd = mysqlServer.getPasswd();

        this.host = ExpressionUtils.getOneValue(mysqlServer.getHost());

        this.port = ExpressionUtils.getOneValue(mysqlServer.getPort());

        this.db = ExpressionUtils.getOneValue(mysqlServer.getDb());

        String dbKey = DBSource.genKey(this.getClass(), this.host, this.port, this.db);

        if (encodingMaps.containsKey(this.encoding)) {
            this.encoding = encodingMaps.get(this.encoding);
        }

        DBSource.register(dbKey, this.genProperties());

        try {

            this.connection = DBSource.getConnection(dbKey);
            stmt = (Statement) ((org.apache.commons.dbcp.DelegatingConnection) this.connection)
                    .getInnermostDelegate().createStatement(
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);

			/* set connect encoding */
            log.info(String.format("Config encoding %s .",
                    this.encoding));
            for (String sql : this.makeLoadEncoding(encoding))
                stmt.execute(sql);

            //reset net_write_timeout and net_read_timeout
            stmt.executeUpdate("SET SESSION net_write_timeout=3600;");
            stmt.executeUpdate("SET SESSION net_read_timeout=3600;");

            //execute preSQL
            for (String subSql : this.location.getPreSql().split(";")) {
                log.info(String.format("run prepare sql %s .",
                        subSql));
                stmt.executeUpdate(subSql);
            }

        } catch (Exception e) {

            throw new OutputException(e.getMessage(), e);
        } finally {
            if (null != this.stmt) {
                try {
                    this.stmt.close();
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
            if (null != this.connection) {
                try {
                    this.connection.close();
                    this.connection = null;
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
        }
    }

    @Override
    public void post(boolean success) {
        if (!success) {
            return;
        }

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        this.location = (MysqlOutputLocation) l;

        if (StringUtils.isBlank(this.location.getPostSql())) {
            return;
        }

        this.encoding = this.outputConfig.getEncode();

        this.concurrency = this.runtimeConfig.getOutputConcurrency();

        MysqlServer mysqlServer = this.location.getMysql();

        this.user = mysqlServer.getUser();

        this.passwd = mysqlServer.getPasswd();

        this.host = ExpressionUtils.getOneValue(mysqlServer.getHost());

        this.port = ExpressionUtils.getOneValue(mysqlServer.getPort());

        this.db = ExpressionUtils.getOneValue(mysqlServer.getDb());

        String dbKey = DBSource.genKey(this.getClass(), this.host, this.port, this.db);

        if (encodingMaps.containsKey(this.encoding)) {
            this.encoding = encodingMaps.get(this.encoding);
        }

        DBSource.register(dbKey, this.genProperties());

        try {

            this.connection = DBSource.getConnection(dbKey);
            stmt = (Statement) ((org.apache.commons.dbcp.DelegatingConnection) this.connection)
                    .getInnermostDelegate().createStatement(
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);

			/* set connect encoding */
            log.info(String.format("Config encoding %s .",
                    this.encoding));
            for (String sql : this.makeLoadEncoding(encoding))
                stmt.execute(sql);

            //reset net_write_timeout and net_read_timeout
            stmt.executeUpdate("SET SESSION net_write_timeout=3600;");
            stmt.executeUpdate("SET SESSION net_read_timeout=3600;");

            //execute postSQL
            for (String subSql : this.location.getPostSql().split(";")) {
                log.info(String.format("run post sql %s .",
                        subSql));
                stmt.executeUpdate(subSql);
            }

        } catch (Exception e) {

            throw new OutputException(e.getMessage(), e);
        } finally {
            if (null != this.stmt) {
                try {
                    this.stmt.close();
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
            if (null != this.connection) {
                try {
                    this.connection.close();
                    this.connection = null;
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
        }
    }

    @Override
    public void init() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!MysqlOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not Hive!");
        }

        location = (MysqlOutputLocation) l;

        this.encoding = this.outputConfig.getEncode();

        MysqlServer mysqlServer = this.location.getMysql();

        this.user = mysqlServer.getUser();

        this.passwd = mysqlServer.getPasswd();

        this.host = ExpressionUtils.getOneValue(mysqlServer.getHost());

        this.port = ExpressionUtils.getOneValue(mysqlServer.getPort());

        this.db = ExpressionUtils.getOneValue(mysqlServer.getDb());

        this.params = mysqlServer.getParams();

        this.set = this.location.getSet();

        this.fieldDelim = this.outputConfig.getSchema().getFieldDelim();

        if (!StringUtils.isBlank(this.set)) {
            this.set = "set " + this.set;
        }

        this.replace = "REPLACE".equalsIgnoreCase(this.location.getReplace()) ? "REPLACE" : "IGNORE";

        this.tbName = ExpressionUtils.getOneValue(this.location.getTbname());
        if (StringUtils.isBlank(this.tbName)) {
            throw new OutputException("table name is empty.");
        }

        this.concurrency = this.runtimeConfig.getOutputConcurrency();

        String dbKey = DBSource.genKey(this.getClass(), this.host, this.port, this.db);

        if (encodingMaps.containsKey(this.encoding)) {
            this.encoding = encodingMaps.get(this.encoding);
        }

        DBSource.register(dbKey, this.genProperties());

        try {

            this.connection = DBSource.getConnection(dbKey);
            stmt = (Statement) ((org.apache.commons.dbcp.DelegatingConnection) this.connection)
                    .getInnermostDelegate().createStatement(
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);

			/* set connect encoding */
            log.info(String.format("Config encoding %s .",
                    this.encoding));
            for (String sql : this.makeLoadEncoding(encoding))
                stmt.execute(sql);

            //reset net_write_timeout and net_read_timeout
            stmt.executeUpdate("SET SESSION net_write_timeout=3600;");
            stmt.executeUpdate("SET SESSION net_read_timeout=3600;");
            /* load data begin */
            loadSql = this.makeLoadSql();
            log.info(String.format("Load sql: %s.", visualSql(loadSql)));

            PipedInputStream pis = new PipedInputStream();
            MysqlOutputAdapter localInputStream = new MysqlOutputAdapter(pis);

            this.pos = new PipedOutputStream(pis);

            stmt.setLocalInfileInputStream(localInputStream);

            Runnable r = new Runnable() {

                @Override
                public void run() {
                    try {
                        stmt.executeUpdate(visualSql(loadSql));
                    } catch (SQLException e) {
                        OutputWorker.OUTPUT_ERROR.put(outputConfig.getId() + "_inner", e);
                        if (null != pos) {
                            try {
                                pos.close();
                            } catch (IOException e1) {
                                log.debug("Mysql output close failed!", e1);
                            }
                        }
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException e1) {
                                log.debug(e.getMessage(), e);
                            }
                        }
                        if (null != connection) {
                            try {
                                connection.close();
                                connection = null;
                            } catch (SQLException e1) {
                                log.debug(e1.getMessage(), e1);
                            }
                        }
                    }
                }

            };

            e = Executors.newFixedThreadPool(1);
            e.execute(r);

        } catch (Exception e) {
            if (null != this.stmt) {
                try {
                    this.stmt.close();
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
            if (null != this.connection) {
                try {
                    this.connection.close();
                    this.connection = null;
                } catch (SQLException e1) {
                    log.debug(e1.getMessage(), e1);
                }
            }
            throw new OutputException(e.getMessage(), e);
        }
    }

    private Properties genProperties() {
        Properties p = new Properties();
        p.setProperty("driverClassName", DRIVER_NAME);
        p.setProperty("url", String.format("jdbc:mysql://%s:%s/%s", this.host,
                this.port, this.db));
        p.setProperty("username", this.user);
        p.setProperty("password", this.passwd);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));

        return p;
    }

    private String quoteData(String data) {
        if (data == null || data.trim().startsWith("@")
                || data.trim().startsWith("`"))
            return data;
        return ('`' + data + '`');
    }

    private String visualSql(String sql) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("\n", "\\n");
        map.put("\t", "\\t");
        map.put("\r", "\\r");
        map.put("\\", "\\\\");

        for (String s : map.keySet()) {
            sql = sql.replace(s, map.get(s));
        }
        return sql;
    }

    // colorder can not be null
    private String splitColumns(String colorder) {
        String[] columns = colorder.split(",");
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            sb.append(quoteData(column.trim()) + ",");
        }
        return sb.substring(0, sb.lastIndexOf(","));
    }

    private String makeLoadSql() {
        String sql = "LOAD DATA LOCAL INFILE '`dpump.output`' "
                + this.replace + " INTO TABLE ";
        // fetch table
        sql += this.quoteData(this.tbName);
        // fetch charset
        sql += " CHARACTER SET " + this.encoding;
        // fetch records
        sql += String.format(" FIELDS TERMINATED BY '\001' ESCAPED BY '\\' ");
        // sql += String.format(" FIELDS TERMINATED BY '%c' ", this.sep);
        // fetch lines
        sql += String.format(" LINES TERMINATED BY '\002' ");
        // add set statement
        sql += this.set;
        sql += ";";
        return sql;
    }

    private List<String> makeLoadEncoding(String encoding) {
        List<String> ret = new ArrayList<String>();

        String configSql = "SET %s=%s; ";
        for (String config : encodingConfigs) {
            log.info(String.format(configSql, config, encoding));
            ret.add(String.format(configSql, config, encoding));
        }

        return ret;
    }

    private static final char[] replaceChars = {'\001', 0, '\002', 0};

    class MysqlOutputAdapter extends InputStream {

        private int lineCounter = 0;
        /* 列分隔符 */
        private char sep = '\001';
        /* 行分隔符 */
        private final char BREAK = '\002';
        /* NULL字面字符*/
        private final String NULL = "\\N";

        private String encode = OracleOutput.this.encoding;

        private Line line = null;
        /* 从line中获取一行数据暂存数组*/
        private byte buffer[] = null;

        private StringBuilder lineBuilder = new StringBuilder(1024 * 1024 * 8);

        /* 存放上次余下 */
        private byte[] previous = new byte[1024 * 1024 * 8];
        /* 上次余留数据长度 */
        private int preLen = 0;
        /* 上次余留数据起始偏移 */
        private int preOff = 0;

        private BufferedReader reader;

        public MysqlOutputAdapter(PipedInputStream pis) throws UnsupportedEncodingException {
            this.reader = new BufferedReader(new InputStreamReader(pis, encode));
        }

        public void close() throws IOException {
            this.reader.close();
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            if (buff == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > buff.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            //System.out.printf(String.format("Len: %d\n", len));
            int total = 0;
            int read = 0;
            while (len > 0) {
                read = this.fetchLine(buff, off, len);
                if (read < 0) {
                    break;
                }
                off += read;
                len -= read;
                total += read;
            }

            if (total == 0)
                return (-1);

            return total;
        }

        private void buildString(Line line) {
            lineBuilder.setLength(0);
            String field;
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line.getLine(), fieldDelim);
            for (int i = 0, num = fields.length;
                 i < num; i++) {
                field = fields[i];
                if (null != field) {
//	                field = field.replace("\\", "\\\\");
                    lineBuilder.append(replaceChars(field, replaceChars));
                } else {
                    lineBuilder.append("\\N");
                }
                if (i < num - 1)
                    lineBuilder.append(this.sep);
                else {
                    lineBuilder.append(this.BREAK);
                }
            }
        }

        private int fetchLine(byte[] buff, int off, int len) throws IOException {
	        /* it seems like I am doing C coding. */
            int ret = 0;
	        /* record current fetch len */
            int currLen;
	        
	        /* 查看上次是否有剩余 */
            if (this.preLen > 0) {
                currLen = Math.min(this.preLen, len);
                System.arraycopy(this.previous, this.preOff, buff, off, currLen);
                this.preOff += currLen;
                this.preLen -= currLen;
                off += currLen;
                len -= currLen;
                ret += currLen;
	            
	            /* 如果buff比较小，上次余下的数据 */
                if (this.preLen > 0) {
                    return ret;
                }
            }
	        
	        /* 本次读数据的逻辑 */
            int lineLen;
            int lineOff = 0;
            String record = this.reader.readLine();
	        
	        /* line为空，表明数据已全部读完 */
            if (record == null) {
                if (ret == 0)
                    return (-1);
                return ret;
            }
            line = new Line(record);

            this.lineCounter++;
            this.buildString(line);
            this.buffer = lineBuilder.toString().getBytes(this.encode);
            lineLen = this.buffer.length;
            currLen = Math.min(lineLen, len);
            System.arraycopy(this.buffer, 0, buff, off, currLen);
            len -= currLen;
            lineOff += currLen;
            lineLen -= currLen;
            ret += currLen;
	        /* len > 0 表明这次fetchLine还没有将buff填充完毕, buff有剩佄1�7 留作下次填充 */
            if (len > 0) {
                return ret;
            }
	        
	        /* 该buffer已经不够放一个line，因此把line的内容保存下来，供下丄1�7次fetch使用 
	         * 这里的假设是previous足够处1�7 绝对够容纳一个line的内宄1�7 */
	        /* fix bug: */
            if (lineLen > this.previous.length) {
                this.previous = new byte[lineLen << 1];
            }
            System.arraycopy(this.buffer, lineOff, this.previous, 0, lineLen);
            this.preOff = 0;
            this.preLen = lineLen;
            return (ret);
        }

        @Override
        public int read() throws IOException {
	        /*
	         * 注意: 没有实现read()
	         * */
            throw new IOException("Read() is not supported");
        }

        public int getLineNumber() {
            return this.lineCounter;
        }

        public String replaceChars(String old, char[] rchars) {
            if (null == rchars)
                return old;

            int oldLen = old.length();
            int rLen = rchars.length;

            StringBuilder sb = new StringBuilder(oldLen);
            char[] oldArrays = old.toCharArray();
            boolean found;
            char c1;

            for (int i = 0; i < oldLen; i++) {
                found = false;
                c1 = oldArrays[i];
                for (int j = 0; j < rLen; j += 2) {
                    if (c1 == rchars[j]) {
                        if (rchars[j + 1] != 0) {
                            sb.append(rchars[j + 1]);
                        }
                        found = true;
                    }
                }
                if (!found) {
                    sb.append(c1);
                }
            }
            return sb.toString();
        }
    }
}