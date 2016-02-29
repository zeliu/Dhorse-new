package cn.wanda.dataserv.input.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.resource.MysqlServer;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.utils.DBUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.MysqlInputLocation;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.DBSource;

/**
 * reminding:<br>
 * use '\t' as field delim in Line read from mysql.<br>
 *
 * @author songqian
 */
@Log4j
public class MysqlInput extends AbstractInput implements Input {

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
    private MysqlInputLocation location;

    private String user = "";

    private String passwd = "";

    private String host = "";

    private String port = "3306";

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

        if (!MysqlInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not Mysql!");
        }

        location = (MysqlInputLocation) l;

        this.encoding = this.inputConfig.getEncode();

        MysqlServer mysqlServer = this.location.getMysql();

        this.user = mysqlServer.getUser();

        this.passwd = mysqlServer.getPasswd();

        this.host = ExpressionUtils.getOneValue(mysqlServer.getHost());

        this.port = ExpressionUtils.getOneValue(mysqlServer.getPort());

        this.db = ExpressionUtils.getOneValue(mysqlServer.getDb());

        this.params = mysqlServer.getParams();

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

        log.info(String.format("mysql input start to query %s .", sql));
        try {
            //donot use mysql query cache
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
//			stmt.executeUpdate("SET SESSION query_cache_type=0;");
//
//			//reset net_write_timeout and net_read_timeout
//			stmt.executeUpdate("SET SESSION net_write_timeout=3600;");
//			stmt.executeUpdate("SET SESSION net_read_timeout=3600;");
            stmt.close();

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);

            com.mysql.jdbc.Statement mysqlStmt = (com.mysql.jdbc.Statement) ((org.apache.commons.dbcp.DelegatingStatement) stmt)
                    .getInnermostDelegate();
            mysqlStmt.enableStreamingResults();

            rs = DBUtils.query(mysqlStmt, sql);
            // init column types
            columnCount = rs.getMetaData().getColumnCount();

            initColumnTypes(rs);

        } catch (SQLException e) {
            throw new InputException(String.format(
                    "mysql input execute query failed %s query:%S.",
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
            log.warn("close mysql input failed.", e);
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
//		} else {
//			TODO : 目前使用MYSQL函数进行拼接,以期提高效率,不过这样会导致难以替换原有字段中的\t字符.这里暂时先不考虑通过这种方式提高读取速度
//			String[] colArray = cols.split(",");
//			StringBuilder sb = new StringBuilder();
//			sb.append(" CONCAT_WS('");
//			sb.append(this.fieldsSeparator);
//			sb.append("', ");
//			for(String col : colArray){
//				sb.append("ifnull(");
//				sb.append(col);
//				sb.append(",'null'),");
//			}
//			sb.delete(sb.length() - 1, sb.length());
//			sb.append(" ) ");
//			
//			cols = sb.toString();
        }
        if (StringUtils.isBlank(tbName)) {
            throw new InputException(
                    "mysql input init failed: table name can not empty!");
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
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/"
                + this.db + "?" + encodeDetail
                + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
                + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
//		url = url + "&traceProtocol=true"
//				+ "&useReadAheadInput=false&useUnbufferedInput=false";

        if (!StringUtils.isBlank(this.params)) {
            url = url + "&" + this.params;
        }

        p.setProperty("username", user);
        p.setProperty("password", passwd);
        p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        p.setProperty("url", url);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));
        p.setProperty("initialSize", String.valueOf(concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        //remove sentence cause need update query_cache_type
//		p.setProperty("defaultReadOnly", "true");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1 from dual");

        log.info(String.format("MysqlReader try connection: %s .", url));
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