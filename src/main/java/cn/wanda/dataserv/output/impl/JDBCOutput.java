package cn.wanda.dataserv.output.impl;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.JDBCOutputLocation;
import cn.wanda.dataserv.config.resource.JDBCConf;
import cn.wanda.dataserv.config.resource.JDBCServer;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.AbstractOutput;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.InterruptedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by liuze on 2015/10/30 0030.
 */
public class JDBCOutput extends AbstractOutput {

    JDBCConf jdbcConf;

    JDBCServer jdbcServer;

    JDBCOutputLocation location;

    protected String type;

    protected String preSql;

    protected String updateSql;

    protected String fieldDelim;

    protected int batchSize;

    protected Connection connection;

    protected Statement statement;

    protected int i=1;


    @Override
    public void writeLine(Line line) {

        String[] twoSql=this.updateSql.split("values");
        String insertSql=twoSql[0];
        String valueSql=" values(";
        String[] columnsType=twoSql[1].replace("(","").replace(")","").trim().split(",");
        String[] str = line.getLine().split(this.fieldDelim);
        for(int i=0;i<columnsType.length;i++){
            String column=str[i].replace("'", "").replace("\\\\N", "").replace("\\N", "").replace("\\","\\\\");
            String column_int=str[i].replace("'", "0").replace("\\\\N", "0").replace("\\N", "0");
            if(columnsType[i].equals("string")){
                valueSql=valueSql+"'"+column+"',";
            }else if(columnsType[i].equals("int")){
                if ("".equals(column.replace(" ","")))
                {
                    valueSql=valueSql+"0,";
                }else{
                    valueSql=valueSql+column_int+",";
                }
            }else if(columnsType[i].equals("date")){
                if ("oracle".equals(type)) {
                    valueSql = valueSql + "to_date('" + column + "', 'yyyy-mm-dd'),";
                }
                if("mysql".equals(type)){
                    valueSql = valueSql + "str_to_date('" + column + "', '%Y-%m-%d'),";
                }
            }else if(columnsType[i].equals("timestamp")){
                if ("oracle".equals(type)) {
                    valueSql = valueSql + "to_date('" + column + "', 'yyyy-mm-dd hh24:mi:ss'),";
                }
                if("mysql".equals(type)){
                    valueSql = valueSql + "str_to_date('" + column.replace("\\N", "") + "', '%Y-%m-%d %H:%i:%s'),";
                }
            }else{
                valueSql=valueSql+"'"+column+"',";
            }

        }

        String executeSql=insertSql+valueSql.substring(0,valueSql.length()-1)+")";
        try {
            statement.addBatch(executeSql);
            if (i % batchSize == 0){
                statement.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        i=i+1;

    }

    @Override
    public void close() {

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void init() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!JDBCOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not JDBC!");
        }

        location = (JDBCOutputLocation) l;
        jdbcServer=location.getJdbcServer();
        jdbcConf = location.getJdbcConf();


        this.preSql = jdbcConf.getPreSql();
        this.updateSql = jdbcConf.getUpdateSql();
        this.fieldDelim = jdbcConf.getFieldDelim();
        this.batchSize = Integer.parseInt(jdbcConf.getBatchSize());
        this.type = location.getType();

        this.connection = getConnection(jdbcServer);
        this.statement = getStatement(this.connection);

        excuteUpdateSql(this.statement, this.preSql);

    }

    @Override
    public void last(boolean success) {
        if(success){

            try {
                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }


    private Connection getConnection(final JDBCServer jdbcServer) {

        Connection connection = null;
        String driver="";
        String url="";
        if(this.type.equals("mysql")){
            driver="com.mysql.jdbc.Driver";
            url="jdbc:mysql://"+jdbcServer.getHost()+":"+jdbcServer.getPort()+"/"+jdbcServer.getDb();
        }else if(this.type.equals("oracle")){
            driver="oracle.jdbc.driver.OracleDriver";
            url="jdbc:oracle:thin:@"+jdbcServer.getHost()+":"+jdbcServer.getPort()+":"+jdbcServer.getDb();
        }else if(this.type.equals("sqlserver")){
            driver="com.microsoft.jdbc.sqlserver.SQLServerDriver";
            url="jdbc:sqlserver://"+jdbcServer.getHost()+":"+jdbcServer.getPort()+";DatabaseName="+jdbcServer.getDb();
        }else{
            driver="com.mysql.jdbc.Driver";
            url="jdbc:mysql://"+jdbcServer.getHost()+":"+jdbcServer.getPort()+"/"+jdbcServer.getDb();
        }
        System.out.println("url:"+url+"driver"+driver);
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            connection = DriverManager.getConnection(url, jdbcServer.getUser(), jdbcServer.getPasswd());
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;

    }

    private Statement getStatement(Connection connection) {


        Statement statement = null;

        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

        return statement;

    }

    private void excuteUpdateSql(Statement statement, String sql) {
        try {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                this.statement.close();
                this.connection.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }




}
