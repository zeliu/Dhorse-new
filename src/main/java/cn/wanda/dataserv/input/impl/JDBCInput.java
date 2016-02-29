package cn.wanda.dataserv.input.impl;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.JDBCLocation;
import cn.wanda.dataserv.config.resource.JDBCContext;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.core.ObjectFactory;
import cn.wanda.dataserv.core.StoreSchema;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.ConvertUtils;
import cn.wanda.dataserv.utils.DBUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static cn.wanda.dataserv.utils.ConvertUtils.readFromJsonStr;
import static cn.wanda.dataserv.utils.ConvertUtils.writeToJsonStr;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;


/**
 * Created by songzhuozhuo on 2015/4/1
 */

@Log4j
public class JDBCInput extends AbstractInput {

    JDBCContext jdbcContext;

    Connection readConnection;

    String user;
    String password;
    String url;
    String sql;

    StoreSchema storeSchema;

    Map<String, String> columnNameMap;

    //列名
    LinkedList<String> keys;

    ResultSet rs;
    ResultSetMetaData metaData;
    int queryTimeOut;

    private int rounding;

    private String fieldsSeparator = "\t";

    private int fetchsize = 10;

    String driver;

    @Override
    public void init() {
        ClassLoader cl = ObjectFactory.dpumpClassLoader.get(JDBCInput.class
                .getName());
        if (cl != null) {
            Thread.currentThread().setContextClassLoader(cl);
        }

        LocationConfig l = this.inputConfig.getLocation();
        if (!JDBCLocation.class.isAssignableFrom(l.getClass())) {
            log.error("input config type is not JDBC!");
            throw new InputException(
                    "input config type is not JDBC!");
        }

        JDBCLocation location = (JDBCLocation) l;
        //jdbccontext
        this.jdbcContext = location.getContext();
        //cloumns ? line
        this.storeSchema = location.getStoreSchema();
        this.url = jdbcContext.getUrl();
        this.user = jdbcContext.getUser();
        this.password = jdbcContext.getPassword();
        this.queryTimeOut = (int) ConvertUtils.getAsTime(jdbcContext.getQueryTimeOut(), TimeValue.timeValueSeconds(0l)).seconds();
        this.fieldsSeparator = jdbcContext.getFieldsSeparator();
        /**
         * fetchsize
         */
        String fetchSizeStr = jdbcContext.getFetchsize();
        if ("min".equals(fetchSizeStr)) {
            fetchsize = Integer.MIN_VALUE; // for MySQL streaming mode
        } else if (fetchSizeStr != null) {
            try {
                fetchsize = Integer.parseInt(fetchSizeStr);
            } catch (Exception e) {
                // ignore unparseable
            }
        } else {
            // if MySQL, enable streaming mode hack by default
            if (url != null && url.startsWith("jdbc:mysql")) {
                fetchsize = Integer.MIN_VALUE; // for MySQL streaming mode
            }
        }

        if (url.startsWith("jdbc:msyql")) {
            driver = "com.mysql.jdbc.driver";
        } else if (url.startsWith("jdbc:oracle")) {
            driver = "oracle.jdbc.driver.OracleDriver";
        } else if (url.startsWith("jdbc:sqlserver")) {
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        try {
            if (jdbcContext.getColumnNameMap() != null) {
                this.columnNameMap = readFromJsonStr(jdbcContext.getColumnNameMap(), HashMap.class);
            }
            this.readConnection = getConnectionForReading();
            execSql(jdbcContext.getSql());
            this.metaData = rs.getMetaData();
            this.rounding = getRounding(jdbcContext.getRounding());
            //columNname
            keys = keys();
        } catch (Exception e) {
            throw new InputException("jdbc input init error", e);
        }
    }

    /**
     * Get JDBC connection for reading
     *
     * @return the connection
     * @throws SQLException when SQL execution gives an error
     */

    public synchronized Connection getConnectionForReading() throws SQLException {
        boolean invalid = readConnection == null || readConnection.isClosed();
        try {
            invalid = invalid || !readConnection.isValid(5);
        } catch (AbstractMethodError e) {
            // old/buggy JDBC driver
            log.debug(e.getMessage());
        } catch (SQLFeatureNotSupportedException e) {
            log.debug(e.getMessage());
        }
        if (invalid) {
            int retries = jdbcContext.getMaxRetries();
            while (retries > 0) {
                retries--;
                try {
                    if (driver != null) {
                        Class.forName(driver);
                    }
                    if (user != null) {
                        Properties properties = new Properties();
                        properties.put("user", user);
                        if (password != null) {
                            properties.put("password", password);
                        }

                        //json 字符串 连接配置项
                        if (jdbcContext.getConnectionProperties() != null) {
                            String pstr = jdbcContext.getConnectionProperties();
                            try {
                                Map<String, Object> connectprop = readFromJsonStr(pstr, HashMap.class);
                                properties.putAll(connectprop);
                            } catch (IOException e) {
                                log.error(e.getMessage());
                            }
                        }
                        readConnection = DriverManager.getConnection(this.url, properties);
                    } else {
                        readConnection = DriverManager.getConnection(this.url);
                    }
                    DatabaseMetaData metaData = readConnection.getMetaData();

                    if (metaData.getTimeDateFunctions().contains("TIMESTAMPDIFF")) {
                        jdbcContext.setTimestampDiffSupported(true);
                    }
                    // "readonly" is required by MySQL for large result streaming
                    readConnection.setReadOnly(true);
                    readConnection.setAutoCommit(jdbcContext.isAutocommit());
                    return readConnection;
                } catch (Exception e) {
                    log.error("while opening read connection: " + url + " " + e.getMessage(), e);
                    try {
                        String rt = jdbcContext.getMaxRetriesWait();
                        TimeValue wait = ConvertUtils.getAsTime(rt, TimeValue.timeValueSeconds(0));
                        log.debug("waiting for" + wait.seconds() + "seconds");
                        Thread.sleep(wait.millis());
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
        return readConnection;
    }

    /**
     * @return
     */
    @Override
    public Line readLine() {
        try {
            if (rs.next()) {
                this.keys = keys();
                int columnCount = metaData.getColumnCount();
                LinkedHashMap<String, Object> colums = new LinkedHashMap<String, Object>();
                List<Object> values = new LinkedList<Object>();
                for (int i = 1; i <= columnCount; i++) {
                    Object value = parseType(rs, i, metaData.getColumnType(i), Locale.getDefault());
                    values.add(value);
                }
                String line;
                if (storeSchema.equals(StoreSchema.COLUMNS)) {
                    meger(colums, keys, values);
                    line = writeToJsonStr(colums);
                } else {
                    line = StringUtils.join(values, fieldsSeparator);
                }
                return new Line(line);
            } else {
                return Line.EOF;
            }
        } catch (Exception e) {
            throw new InputException(e);
        }
    }

    /**
     * meger key value
     *
     * @param colums
     * @param keys
     * @param values
     */
    private void meger(LinkedHashMap<String, Object> colums, List<String> keys, List<Object> values) {
        if (keys.size() != values.size()) {
            log.error(String.format("keys size [%d] values size [%d] not equals!", keys.size(), values.size()));
        }
        try {
            keys = keys();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int size = values.size();
        for (int i = 0; i < size; i++) {
            colums.put(keys.get(i), values.get(i));
        }
    }

    /**
     *
     */
    @Override
    public void close() {
        try {
            if (rs != null) {
                DBUtils.closeResultSet(rs);
            }
            if (readConnection != null) {
                readConnection.close();
            }
            readConnection = null;
        } catch (SQLException e) {
            log.warn("close jdbc input failed.", e);
        }
    }

    /**
     * @param sql
     * @throws Exception
     */
    private void execSql(String sql) throws Exception {
        Statement statement = null;
        readConnection = getConnectionForReading();

        if (readConnection != null) {
            statement = readConnection.createStatement();
            try {
                statement.setQueryTimeout(this.queryTimeOut);
            } catch (SQLFeatureNotSupportedException e) {
                log.warn("driver does not support setQueryTimeout(), skipped");
            }
            this.rs = executeQuery(statement, sql);
        }
    }

    /**
     * Execute query statement
     *
     * @param statement the statement
     * @param sql       the SQL
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    private ResultSet executeQuery(Statement statement, String sql) throws SQLException {
        statement.setMaxRows(jdbcContext.getMaxRows());
        statement.setFetchSize(fetchsize);
        return statement.executeQuery(sql);
    }

    private LinkedList<String> keys() throws SQLException {
        if (keys == null) {
            keys = new LinkedList<String>();
            int columns = metaData.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                String columnname = metaData.getColumnLabel(i);
                if (columnNameMap != null && columnNameMap.containsKey(columnname)) {
                    keys.add(columnNameMap.get(columnname));
                } else {
                    keys.add(columnname);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("columns: %s", StringUtils.join(keys, ",")));
            }
        }
        return keys;
    }


    /**
     * Parse of value of result set
     *
     * @param result the result set
     * @param i      the offset in the result set
     * @param type   the JDBC type
     * @param locale the locale to use for parsing
     * @return The parse value
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/output error occurs
     */
    public Object parseType(ResultSet result, Integer i, int type, Locale locale)
            throws SQLException, IOException, ParseException {
        log.trace(String.format("i=%d type=%s", i, type));
        switch (type) {

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                return result.getString(i);
            }
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                return result.getNString(i);
            }

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                byte[] b = result.getBytes(i);
                return jdbcContext.isTreatBinaryAsString() ? (b != null ? new String(b) : null) : b;
            }

            case Types.ARRAY: {
                Array arr = result.getArray(i);
                return arr == null ? null : arr.getArray();
            }

            case Types.BIGINT: {
                Object o = result.getLong(i);
                return result.wasNull() ? null : o;
            }

            case Types.BIT: {
                try {
                    Object o = result.getInt(i);
                    return result.wasNull() ? null : o;
                } catch (Exception e) {
                    String exceptionClassName = e.getClass().getName();
                    // postgresql can not handle boolean, it will throw PSQLException, something like "Bad value for type int : t"
                    if ("org.postgresql.util.PSQLException".equals(exceptionClassName)) {
                        return "t".equals(result.getString(i));
                    }
                    throw new IOException(e);
                }
            }

            case Types.BOOLEAN: {
                return result.getBoolean(i);
            }

            case Types.BLOB: {
                Blob blob = result.getBlob(i);
                if (blob != null) {
                    long n = blob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process blob larger than Integer.MAX_VALUE");
                    }
                    byte[] tab = blob.getBytes(1, (int) n);
                    blob.free();
                    return tab;
                }
                break;
            }

            case Types.CLOB: {
                Clob clob = result.getClob(i);
                if (clob != null) {
                    long n = clob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process clob larger than Integer.MAX_VALUE");
                    }
                    String str = clob.getSubString(1, (int) n);
                    clob.free();
                    return str;
                }
                break;
            }
            case Types.NCLOB: {
                NClob nclob = result.getNClob(i);
                if (nclob != null) {
                    long n = nclob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process nclob larger than Integer.MAX_VALUE");
                    }
                    String str = nclob.getSubString(1, (int) n);
                    nclob.free();
                    return str;
                }
                break;
            }

            case Types.DATALINK: {
                return result.getURL(i);
            }

            case Types.DATE: {
                try {
                    Date d = result.getDate(i);
                    return d != null ? formatDate(d.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            }
            case Types.TIME: {
                try {
                    Time t = result.getTime(i);
                    return t != null ? formatDate(t.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            }
            case Types.TIMESTAMP: {
                try {
                    Timestamp t = result.getTimestamp(i);
                    return t != null ? formatDate(t.getTime()) : null;
                } catch (SQLException e) {
                    // java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column ... to TIMESTAMP.
                    return null;
                }
            }

            case Types.DECIMAL:
            case Types.NUMERIC: {
                BigDecimal bd = null;
                try {
                    // getBigDecimal() should get obsolete. Most seem to use getString/getObject anyway...
                    bd = result.getBigDecimal(i);
                } catch (NullPointerException e) {
                    // But is it true? JDBC NPE exists since 13 years?
                    // http://forums.codeguru.com/archive/index.php/t-32443.html
                    // Null values are driving us nuts in JDBC:
                    // http://stackoverflow.com/questions/2777214/when-accessing-resultsets-in-jdbc-is-there-an-elegant-way-to-distinguish-betwee
                }
                if (bd == null || result.wasNull()) {
                    return null;
                }
                if (jdbcContext.getScale() >= 0) {
                    bd = bd.setScale(jdbcContext.getScale(), this.rounding);
                    try {
                        long l = bd.longValueExact();
                        if (Long.toString(l).equals(result.getString(i))) {
                            // convert to long if possible
                            return l;
                        } else {
                            // convert to double (with precision loss)
                            return bd.doubleValue();
                        }
                    } catch (ArithmeticException e) {
                        return bd.doubleValue();
                    }
                } else {
                    return bd.toPlainString();
                }
            }

            case Types.DOUBLE: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }

            case Types.FLOAT: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }

            case Types.OTHER:
            case Types.JAVA_OBJECT: {
                return result.getObject(i);
            }

            case Types.REAL: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER: {
                try {
                    Integer integer = result.getInt(i);
                    return result.wasNull() ? null : integer;
                } catch (SQLDataException e) {
                    Long l = result.getLong(i);
                    return result.wasNull() ? null : l;
                }
            }

            case Types.SQLXML: {
                SQLXML xml = result.getSQLXML(i);
                return xml != null ? xml.getString() : null;
            }

            case Types.NULL: {
                return null;
            }

            case Types.DISTINCT: {
                log.warn("JDBC type not implemented:" + type);
                return null;
            }

            case Types.STRUCT: {
                log.warn("JDBC type not implemented: " + type);
                return null;
            }
            case Types.REF: {
                log.warn("JDBC type not implemented: " + type);
                return null;
            }
            case Types.ROWID: {
                log.warn("JDBC type not implemented: " + type);
                return null;
            }
            default: {
                log.warn("unknown JDBC type ignored: " + type);
                return null;
            }
        }
        return null;
    }

    private int toJDBCType(String type) {
        if (type == null) {
            return Types.NULL;
        } else if (type.equalsIgnoreCase("NULL")) {
            return Types.NULL;
        } else if (type.equalsIgnoreCase("TINYINT")) {
            return Types.TINYINT;
        } else if (type.equalsIgnoreCase("SMALLINT")) {
            return Types.SMALLINT;
        } else if (type.equalsIgnoreCase("INTEGER")) {
            return Types.INTEGER;
        } else if (type.equalsIgnoreCase("BIGINT")) {
            return Types.BIGINT;
        } else if (type.equalsIgnoreCase("REAL")) {
            return Types.REAL;
        } else if (type.equalsIgnoreCase("FLOAT")) {
            return Types.FLOAT;
        } else if (type.equalsIgnoreCase("DOUBLE")) {
            return Types.DOUBLE;
        } else if (type.equalsIgnoreCase("DECIMAL")) {
            return Types.DECIMAL;
        } else if (type.equalsIgnoreCase("NUMERIC")) {
            return Types.NUMERIC;
        } else if (type.equalsIgnoreCase("BIT")) {
            return Types.BIT;
        } else if (type.equalsIgnoreCase("BOOLEAN")) {
            return Types.BOOLEAN;
        } else if (type.equalsIgnoreCase("BINARY")) {
            return Types.BINARY;
        } else if (type.equalsIgnoreCase("VARBINARY")) {
            return Types.VARBINARY;
        } else if (type.equalsIgnoreCase("LONGVARBINARY")) {
            return Types.LONGVARBINARY;
        } else if (type.equalsIgnoreCase("CHAR")) {
            return Types.CHAR;
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            return Types.VARCHAR;
        } else if (type.equalsIgnoreCase("LONGVARCHAR")) {
            return Types.LONGVARCHAR;
        } else if (type.equalsIgnoreCase("DATE")) {
            return Types.DATE;
        } else if (type.equalsIgnoreCase("TIME")) {
            return Types.TIME;
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            return Types.TIMESTAMP;
        } else if (type.equalsIgnoreCase("CLOB")) {
            return Types.CLOB;
        } else if (type.equalsIgnoreCase("BLOB")) {
            return Types.BLOB;
        } else if (type.equalsIgnoreCase("ARRAY")) {
            return Types.ARRAY;
        } else if (type.equalsIgnoreCase("STRUCT")) {
            return Types.STRUCT;
        } else if (type.equalsIgnoreCase("REF")) {
            return Types.REF;
        } else if (type.equalsIgnoreCase("DATALINK")) {
            return Types.DATALINK;
        } else if (type.equalsIgnoreCase("DISTINCT")) {
            return Types.DISTINCT;
        } else if (type.equalsIgnoreCase("JAVA_OBJECT")) {
            return Types.JAVA_OBJECT;
        } else if (type.equalsIgnoreCase("SQLXML")) {
            return Types.SQLXML;
        } else if (type.equalsIgnoreCase("ROWID")) {
            return Types.ROWID;
        }
        return Types.OTHER;
    }


    public int getRounding(String rounding) {
        if (rounding == null) {
            this.rounding = BigDecimal.ROUND_UP;
        } else {
            if ("ceiling".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_CEILING;
            } else if ("down".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_DOWN;
            } else if ("floor".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_FLOOR;
            } else if ("halfdown".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_HALF_DOWN;
            } else if ("halfeven".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_HALF_EVEN;
            } else if ("halfup".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_HALF_UP;
            } else if ("unnecessary".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_UNNECESSARY;
            } else if ("up".equalsIgnoreCase(rounding)) {
                this.rounding = BigDecimal.ROUND_UP;
            }
        }
        return this.rounding;
    }


    static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    private static String formatDate(long millis) {
        return dateTimeFormatter.print(millis);
    }

    private static String formDateWithTimeZone(long mills) {
        return new DateTime(mills).toString();
    }

    public static void main(String[] args) {
        System.out.println(formatDate(new java.util.Date().getTime()));
        System.out.println(formDateWithTimeZone(new java.util.Date().getTime()));
    }
}
