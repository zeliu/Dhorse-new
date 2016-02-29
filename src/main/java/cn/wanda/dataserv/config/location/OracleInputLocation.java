package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.resource.MysqlServer;
import cn.wanda.dataserv.config.resource.OracleServer;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.core.StoreSchema;
import cn.wanda.dataserv.utils.DBSource;
import cn.wanda.dataserv.utils.DBUtils;
import cn.wanda.dataserv.utils.PathPatternMatcher;
import cn.wanda.dataserv.utils.StringUtil;
import javolution.text.CharSet;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * Created by songzhuozhuo on 2015/2/13
 */
@Data
@Log4j
public class OracleInputLocation extends LocationConfig {
    @Config(name = "oracle")
    private OracleServer oracle;

    @Config(name = "table", require = RequiredType.OPTIONAL)
    private Expression tbname;

    /**
     * input cols, clause after select in sql statements. default is *
     */
    @Config(name = "cols", require = RequiredType.OPTIONAL)
    private String cols = " * ";

    @Config(name = "ServerEncoding", require = RequiredType.OPTIONAL)
    private String ServerEncoding = "utf-8";
    /**
     * where clause in sql statement
     */
    @Config(name = "condition", require = RequiredType.OPTIONAL)
    private Expression condition;

    /**
     * a custom SQL statement for special requirements<br>
     * defaule is empty<br>
     * if param <b>sql</b> is not blank, we will ignore params <b>tbname</b>,<b>cols</b> and <b>where</b>.
     */
    @Config(name = "sql", require = RequiredType.OPTIONAL)
    private Expression sql;

    /**
     * a custom SQL statement before data transmission
     */
    @Config(name = "pre-sql", require = RequiredType.OPTIONAL)
    private String preSql;

    /**
     * a custom SQL statement after data transmission
     */
    @Config(name = "post-sql", require = RequiredType.OPTIONAL)
    private String postSql;


//    @Override
//    protected List<Object> doSplit() {
//        LinkedList<Object> results = new LinkedList<Object>();
//        results.offer(cloneObject(this));
//        LinkedList<OracleServer> servers = new LinkedList<OracleServer>();
//        servers.offer(cloneObject(this.oracle));
//        try {
//            splitDbs(results, servers);
//            log.debug(results.size());
//            splitTbName(results);
//            log.debug(results.size());
//        } catch (Exception e) {
//            log.error(e);
//            throw new ConfigParseException("do split failed.", e);
//        }
//        return results;
//    }
//
//
//    /**
//     * get dbs by "*" and expression
//     *
//     * @param result
//     * @param servers
//     * @throws IllegalAccessException
//     * @throws NoSuchFieldException
//     * @throws SQLException
//     */
//    private void splitDbs(LinkedList<Object> result,
//                          LinkedList<OracleServer> servers)
//            throws IllegalAccessException, NoSuchFieldException, SQLException {
//        //do nothing
//    }
//
//    /**
//     * get tbName by regex and expression
//     *
//     * @param result
//     * @throws IllegalAccessException
//     * @throws NoSuchFieldException
//     * @throws SQLException
//     */
//    private void splitTbName(LinkedList<Object> result)
//            throws IllegalAccessException, NoSuchFieldException, SQLException {
//
//        Connection conn = null;
//        ResultSet rs = null;
//
//        int qSize = result.size();
//        for (int i = 0; i < qSize; i++) {
//            OracleInputLocation oracleLocation = (OracleInputLocation) result.poll();
//            String dbName = ExpressionUtils.getOneValue(oracleLocation.getOracle().getDb());
//            String tbName = ExpressionUtils.getOneValue(oracleLocation.getTbname());
//            if (StringUtils.isEmpty(tbName)) {
//                result.offer(oracleLocation);
//                continue;
//            }
//
//            Pattern p = null;
//            try {
//                p = Pattern.compile(tbName);
//            } catch (PatternSyntaxException e) {
//                result.offer(oracleLocation);
//                continue;
//            }
//
//            // try to get match pattern tbName
//            try {
//                conn = getCoon(oracleLocation.getOracle());
//                DatabaseMetaData dmd = conn.getMetaData();
//                rs = dmd.getTables("null", "%", "%", new String[]{"TABLE"});
//                while (rs.next()) {
//                    String tb = rs.getString("TABLE_NAME");
//                    Matcher m = p.matcher(tb);
//                    if (!m.matches()) {
//                        continue;
//                    }
//                    OracleInputLocation r = cloneObject(oracleLocation);
//                    r.setTbname(new StringConstant(tb));
//                    result.offer(r);
//                }
//            } catch (SQLException e) {
//                throw new ConfigParseException("get table names from source failed", e);
//            } finally {
//                if (rs != null) {
//                    DBUtils.closeResultSet(rs);
//                }
//                if (conn != null) {
//                    conn.close();
//                }
//                conn = null;
//            }
//        }
//    }
//
//    private String getTableNamePattern(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
//        if (databaseMetaData.storesLowerCaseIdentifiers()) {
//            return StringUtils.lowerCase(tableName);
//        } else if (databaseMetaData.storesUpperCaseIdentifiers()) {
//            return StringUtils.upperCase(tableName);
//        } else {
//            return tableName;
//        }
//    }
//    private Connection getCoon(OracleServer s){
//        String host = ExpressionUtils.getOneValue(s.getHost());
//        String port = ExpressionUtils.getOneValue(s.getPort());
//
//        String dbKey = DBSource.genKey(this.getClass(), host, port,
//                "");
//        DBSource.register(dbKey, createProperties(s));
//        return DBSource.getConnection(dbKey);
//
//    }
//    private Statement getStatement(OracleServer s, Connection conn) throws SQLException {
//        // get stmt
//        conn = getCoon(s);
//
//        Statement stmt = conn.createStatement(
//                ResultSet.TYPE_FORWARD_ONLY,
//                ResultSet.CONCUR_READ_ONLY);
////        stmt.setFetchSize(Integer.MIN_VALUE);
//
//        return stmt;
//    }
//
//    private static Properties createProperties(OracleServer s) {
//        Properties p = new Properties();
//
//        String encodeDetail = "";
//
//        String host = ExpressionUtils.getOneValue(s.getHost());
//        String port = ExpressionUtils.getOneValue(s.getPort());
//        String dbname = ExpressionUtils.getOneValue(s.getDb());
//        String url = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + dbname;
//
//        log.info(url);
////                + "/?" + encodeDetail
////                + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
////                + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
//        if (!StringUtils.isBlank(s.getParams())) {
//            url = url + "&" + s.getParams();
//        }
//
//        p.setProperty("username", s.getUser());
//        p.setProperty("password", s.getPasswd());
//        p.setProperty("driverClassName", "oracle.jdbc.driver.OracleDriver");
//        p.setProperty("url", url);
//        p.setProperty("maxActive", String.valueOf(5));
//        p.setProperty("initialSize", String.valueOf(1));
//        p.setProperty("maxIdle", "1");
//        p.setProperty("maxWait", "1000");
//        p.setProperty("testOnBorrow", "true");
//        p.setProperty("validationQuery", "select 1 from dual");
//
//        return p;
//    }
//
//    private <T, V> void cartesian(Queue<T> target, Field field,
//                                  List<V> fieldValue) throws IllegalArgumentException,
//            IllegalAccessException {
//        int qSize = target.size();
//        field.setAccessible(true);
//
//        for (int i = 0; i < qSize; i++) {
//            T t = target.poll();
//
//            for (V value : fieldValue) {
//                T r = cloneObject(t);
//                field.set(r, value);
//                target.offer(r);
//            }
//        }
//    }
//
//    private <T> T cloneObject(T c) {
//        try {
//            return (T) BeanUtils.cloneBean(c);
//        } catch (Exception e) {
//            throw new ElParseException(e);
//        }
//    }

}
