package cn.wanda.dataserv.input.impl;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.SqlserverInputLocation;
import cn.wanda.dataserv.config.resource.SqlserverServer;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.DBSource;
import cn.wanda.dataserv.utils.DBUtils;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * reminding:<br>
 * use '\t' as field delim in Line read from sqlserver.<br>
 *
 * @author liuze
 */
@Log4j
public class SqlserverInput extends AbstractInput implements Input {

    private static final String sql_select_key_word = " select ";
    private static final String sql_from_key_word = " from ";
    private static final String sql_where_key_word = " where ";

    private static final String LF = "\n";
    private static final String ESCAPE_LF = "\\\\n";
    private static final String HT = "\t";
    private static final String ESCAPE_HT = "\\\\t";


    private static Map<String, String> dateFormatMap = new HashMap<String, String>();

    static {
        dateFormatMap.clear();
        dateFormatMap.put("datetime", "yyyy-MM-dd HH:mm:ss");
        dateFormatMap.put("timestamp", "yyyy-MM-dd HH:mm:ss");
        dateFormatMap.put("time", "HH:mm:ss");
    }

    // input params
    private SqlserverInputLocation location;

    private String user = "";

    private String passwd = "";

    private String host = "";

    private String port = "1433";

    private String db = null;

    private String params;

    private String sql = null;

    private int concurrency;

    //
    private Connection conn;

    private ResultSet rs;

    private SimpleDateFormat[] timeMap = null;

    private int columnCount;

    private String fieldsSeparator = "\t";

    @Override
    public void init() {

        // process params
        LocationConfig l = this.inputConfig.getLocation();

        if (!SqlserverInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not sqlserver!");
        }

        location = (SqlserverInputLocation) l;

        this.encoding = this.inputConfig.getEncode();

        SqlserverServer sqlserverServer = this.location.getSqlserver();

        this.user = sqlserverServer.getUser();

        this.passwd = sqlserverServer.getPasswd();

        this.host = ExpressionUtils.getOneValue(sqlserverServer.getHost());

        this.port = ExpressionUtils.getOneValue(sqlserverServer.getPort());

        this.db = ExpressionUtils.getOneValue(sqlserverServer.getDb());

        this.params = sqlserverServer.getParams();

        this.concurrency = this.runtimeConfig.getInputConcurrency();

        this.fieldsSeparator = this.inputConfig.getSchema().getFieldDelim();

        // get real sql
        this.sql = getSql();

        // register dbsource
        Properties p = createProperties();

        String dbKey = DBSource.genKey(this.getClass(), this.host,
                this.port, this.db);
        DBSource.register(dbKey, p);

        // get resultset
        this.conn = DBSource.getConnection(dbKey);

        log.info(String.format("sqlserver input start to query %s .", sql));
        try {
            //donot use sqlserver query cache
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            //stmt.executeUpdate("SET SESSION query_cache_type=0;");

            //reset net_write_timeout and net_read_timeout
            //stmt.executeUpdate("SET SESSION net_write_timeout=3600;");
            //stmt.executeUpdate("SET SESSION net_read_timeout=3600;");
            stmt.close();

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            //stmt.setFetchSize(Integer.MIN_VALUE);

            Statement sqlserverStmt = (Statement) ((org.apache.commons.dbcp.DelegatingStatement) stmt)
                    .getInnermostDelegate();
            //sqlserverStmt.enableStreamingResults();

            rs = DBUtils.query(sqlserverStmt, sql);
            // init column types
            columnCount = rs.getMetaData().getColumnCount();

            initColumnTypes(rs);

        } catch (SQLException e) {
            throw new InputException(String.format(
                    "sqlserver input execute query failed %s query:%S.",
                    e.getMessage(), sql), e);
        }
    }

    @Override
    public Line readLine() {
        try {
            if (rs.next()) {

                String item = null;
                Timestamp ts = null;
                StringBuffer sb = new StringBuffer();
                try {
                    for (int i = 1; i <= columnCount; i++) {
                        if (null != timeMap[i]) {
                            ts = rs.getTimestamp(i);
                            if (null != ts) {
                                item = timeMap[i].format(ts);
                            } else {
                                item = null;
                            }
                        } else {
                            item = rs.getString(i);
                        }
                        if (item != null) {
                            item = item.replaceAll(HT, ESCAPE_HT);
                        }
                        if (item != null) {
                            sb.append(item);
                        }
                        sb.append(fieldsSeparator);
                    }

                } catch (SQLException e) {
                    log.error(e.getMessage() + "| One dirty line : "
                            + sb.toString());
                }
                sb.delete(sb.length() - fieldsSeparator.length(), sb.length());
                String line = sb.toString();
                line = line.replaceAll(LF, ESCAPE_LF);
                return new Line(line);
            } else {
                return Line.EOF;
            }
        } catch (Exception e) {
            throw new InputException(e);
        }

    }

    @Override
    public void close() {
        try {
            if (rs != null) {
                DBUtils.closeResultSet(rs);
            }
            if (conn != null) {
                conn.close();
            }
            conn = null;
        } catch (SQLException e) {
            log.warn("close sqlserver input failed.", e);
        }
    }

    private String getSql() {
        String sql = ExpressionUtils.getOneValue(this.location.getSql());
        if (StringUtils.isNotBlank(sql)) {
            return sql;
        }

        String cols = this.location.getCols();
        String tbName = ExpressionUtils.getOneValue(this.location.getTbname());
        String condition = ExpressionUtils.getOneValue(this.location.getCondition());

        if (StringUtils.isBlank(cols) || "*".equals(cols.trim())) {
            cols = " * ";
        }
        if (StringUtils.isBlank(tbName)) {
            throw new InputException(
                    "sqlserver input init failed: table name can not empty!");
        }
        sql = sql_select_key_word + cols + sql_from_key_word + tbName;
        if (StringUtils.isNotBlank(condition)) {
            sql = sql + sql_where_key_word + condition;
        }

        return sql;
    }

    private Properties createProperties() {
        Properties p = new Properties();

        String encodeDetail = "";

        if (!StringUtils.isBlank(this.encoding)) {
            encodeDetail = "useUnicode=true&characterEncoding=" + this.encoding
                    + "&";
        }

        //String db = ExpressionUtils.getOneValue(s.getDb());
        String url = "jdbc:sqlserver://" + host + ":" + port + "; DatabaseName=" + db;
        if (!StringUtils.isBlank(this.params)) {
            url = url + "&" + this.params;
        }

        p.setProperty("username", user);
        p.setProperty("password", passwd);
        p.setProperty("driverClassName", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        p.setProperty("url", url);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));
        p.setProperty("initialSize", String.valueOf(concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        //remove sentence cause need update query_cache_type
//		p.setProperty("defaultReadOnly", "true");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1 ");

        log.info(String.format("SqlserverReader try connection: %s .", url));
        return p;
    }

    private void initColumnTypes(ResultSet resultSet) throws SQLException {
        timeMap = new SimpleDateFormat[columnCount + 1];

        ResultSetMetaData rsmd = resultSet.getMetaData();

        for (int i = 1; i <= columnCount; i++) {
            String type = rsmd.getColumnTypeName(i).toLowerCase().trim();
            if (dateFormatMap.containsKey(type)) {
                timeMap[i] = new SimpleDateFormat(dateFormatMap.get(type));
            }
        }
    }
}