package cn.wanda.dataserv.config.location;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.el.ElParseException;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.resource.MysqlServer;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.utils.DBUtils;
import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.utils.DBSource;
import cn.wanda.dataserv.utils.PathPatternMatcher;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import lombok.Data;
import lombok.extern.log4j.Log4j;

@Data
@Log4j
public class MysqlInputLocation extends LocationConfig {

    @Config(name = "mysql")
    private MysqlServer mysql;

    @Config(name = "table", require = RequiredType.OPTIONAL)
    private Expression tbname;

    /**
     * input cols, clause after select in sql statements. default is *
     */
    @Config(name = "cols", require = RequiredType.OPTIONAL)
    private String cols = " * ";

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

    /**
     * split by {hosts}x{ports}x{dbs}x{sqls or {tbnames}x{conditions}}
     */
    @Override
    public List<Object> doSplit() {
        LinkedList<Object> result = new LinkedList<Object>();
        result.offer(cloneObject(this));

        LinkedList<MysqlServer> servers = new LinkedList<MysqlServer>();
        servers.offer(cloneObject(this.mysql));

        try {
//			splitDbs(result, servers);
//			splitTbName(result);
        } catch (Exception e) {
            throw new ConfigParseException("do split failed.", e);
        }

        return result;
    }

    /**
     * get dbs by "*" and expression
     *
     * @param result
     * @param servers
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     * @throws SQLException
     */
    private void splitDbs(LinkedList<Object> result,
                          LinkedList<MysqlServer> servers)
            throws IllegalAccessException, NoSuchFieldException, SQLException {

        Connection conn = null;
        ResultSet rs = null;

        int qSize = servers.size();
        for (int i = 0; i < qSize; i++) {
            MysqlServer s = servers.poll();
            String dbName = ExpressionUtils.getOneValue(s.getDb());
            if (PathPatternMatcher.isPattern(dbName)) {
                // try to get match pattern db
                try {
                    Statement stmt = getStatement(s, conn);
                    rs = DBUtils.query(stmt, "show databases");

                    while (rs.next()) {
                        String db = rs.getString(1);
                        if (!PathPatternMatcher.match(dbName, db)) {
                            continue;
                        }
                        MysqlServer r = cloneObject(s);
                        r.setDb(new StringConstant(db));
                        servers.offer(r);
                    }
                } catch (SQLException e) {
                    throw new ConfigParseException("get dbs names from source failed", e);
                } finally {
                    if (rs != null) {
                        DBUtils.closeResultSet(rs);
                    }
                    if (conn != null) {
                        conn.close();
                    }
                    conn = null;
                }
            } else {
                servers.offer(s);
            }
        }

        // handle location
        cartesian(result, this.getClass().getDeclaredField("mysql"), servers);
    }

    /**
     * get tbName by regex and expression
     *
     * @param result
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     * @throws SQLException
     */
    private void splitTbName(LinkedList<Object> result)
            throws IllegalAccessException, NoSuchFieldException, SQLException {

        Connection conn = null;
        ResultSet rs = null;


        int qSize = result.size();
        for (int i = 0; i < qSize; i++) {
            MysqlInputLocation mysqlLocation = (MysqlInputLocation) result.poll();
            String dbName = ExpressionUtils.getOneValue(mysqlLocation.getMysql().getDb());
            String tbName = ExpressionUtils.getOneValue(mysqlLocation.getTbname());
            Pattern p = null;
            try {
                p = Pattern.compile(tbName);
            } catch (PatternSyntaxException e) {
                result.offer(mysqlLocation);
                continue;
            }
            // try to get match pattern tbName
            try {
                Statement stmt = getStatement(mysqlLocation.getMysql(), conn);
                stmt.executeUpdate("use " + dbName);
                rs = DBUtils.query(stmt, "show tables");

                while (rs.next()) {
                    String tb = rs.getString(1);
                    Matcher m = p.matcher(tb);
                    if (!m.matches()) {
                        continue;
                    }
                    MysqlInputLocation r = cloneObject(mysqlLocation);
                    r.setTbname(new StringConstant(tb));
                    result.offer(r);
                }
            } catch (SQLException e) {
                throw new ConfigParseException("get table names from source failed", e);
            } finally {
                if (rs != null) {
                    DBUtils.closeResultSet(rs);
                }
                if (conn != null) {
                    conn.close();
                }
                conn = null;
            }
        }
    }

    private Statement getStatement(MysqlServer s, Connection conn) throws SQLException {
        String host = ExpressionUtils.getOneValue(s.getHost());
        String port = ExpressionUtils.getOneValue(s.getPort());

        String dbKey = DBSource.genKey(this.getClass(), host, port,
                "");
        DBSource.register(dbKey, createProperties(s));

        // get stmt
        conn = DBSource.getConnection(dbKey);

        Statement stmt = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);

        return stmt;
    }

    private <T, V> void cartesian(Queue<T> target, Field field,
                                  List<V> fieldValue) throws IllegalArgumentException,
            IllegalAccessException {

        int qSize = target.size();
        field.setAccessible(true);

        for (int i = 0; i < qSize; i++) {
            T t = target.poll();

            for (V value : fieldValue) {
                T r = cloneObject(t);
                field.set(r, value);
                target.offer(r);
            }
        }
    }

    private static Properties createProperties(MysqlServer s) {
        Properties p = new Properties();

        String encodeDetail = "";

        String host = ExpressionUtils.getOneValue(s.getHost());
        String port = ExpressionUtils.getOneValue(s.getPort());
        String url = "jdbc:mysql://" + host + ":" + port + "/?" + encodeDetail
                + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
                + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);

        if (!StringUtils.isBlank(s.getParams())) {
            url = url + "&" + s.getParams();
        }

        p.setProperty("username", s.getUser());
        p.setProperty("password", s.getPasswd());
        p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        p.setProperty("url", url);
        p.setProperty("maxActive", String.valueOf(5));
        p.setProperty("initialSize", String.valueOf(1));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1 from dual");

        return p;
    }

    private <T> T cloneObject(T c) {
        try {
            return (T) BeanUtils.cloneBean(c);
        } catch (Exception e) {
            throw new ElParseException(e);
        }
    }
}