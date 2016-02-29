package cn.wanda.dataserv.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DBUtils {
    private DBUtils() {
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @return a {@link ResultSet}
     * @param        conn Database connection .
     * @param        sql sql statement to be executed
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        return query(stmt, sql);
    }


    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @return a {@link ResultSet}
     * @param    stmt {@link Statement}
     * @param    sql sql statement to be executed
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Statement stmt, String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this {@link ResultSet}
     *
     * @param    rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                rs.close();
                if (null != stmt) {
                    stmt.close();
                    stmt = null;
                }
            }
            rs = null;
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}