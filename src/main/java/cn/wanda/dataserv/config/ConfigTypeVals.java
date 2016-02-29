package cn.wanda.dataserv.config;

import cn.wanda.dataserv.config.location.*;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.impl.SqlserverInput;
import cn.wanda.dataserv.input.impl.*;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.output.impl.*;
import cn.wanda.dataserv.config.location.DummyLocation;
import cn.wanda.dataserv.config.location.FtpInputLocation;
import cn.wanda.dataserv.config.location.FtpOutputLocation;
import cn.wanda.dataserv.config.location.HdfsInputLocation;
import cn.wanda.dataserv.config.location.HdfsOutputLocation;
import cn.wanda.dataserv.config.location.HttpInputLocation;
import cn.wanda.dataserv.config.location.LocalFileLocation;
import cn.wanda.dataserv.config.location.MysqlInputLocation;
import cn.wanda.dataserv.config.location.MysqlOutputLocation;
import cn.wanda.dataserv.input.impl.DfsInput;
import cn.wanda.dataserv.input.impl.FileInput;
import cn.wanda.dataserv.input.impl.FtpInput;
import cn.wanda.dataserv.input.impl.HttpInput;
import cn.wanda.dataserv.input.impl.MysqlInput;
import cn.wanda.dataserv.output.impl.DummyOutput;


public enum ConfigTypeVals {

    FTP("ftp", FtpInputLocation.class, FtpOutputLocation.class, FtpInput.class, FtpOutput.class),
    LOCALFILE("file", LocalFileLocation.class, LocalFileLocation.class, FileInput.class, FileOutput.class),
    HDFS("hdfs", HdfsInputLocation.class, HdfsOutputLocation.class, DfsInput.class, DfsOutput.class),
    HDP("hdp", null, HdpOutputLocation.class, null, HdpOutput.class),
    HTTP("http", HttpInputLocation.class, null, HttpInput.class, null),
    HIVE("hive", HiveInputLocation.class, HiveOutputLocation.class, null, HiveOutput.class),
    HIVE2("hive2", Hql2InputLocation.class, Hive2OutputLocation.class, Hql2Input.class, Hive2Output.class),
    HQL("hql", HqlInputLocation.class, null, HqlInput.class, null),
    HDPHIVE("hdphive", HdpHqlInputLocation.class, HdpHiveOutputLocation.class, HdpHqlInput.class, HdpHiveOutput.class),
    MYSQL("mysql", MysqlInputLocation.class, MysqlOutputLocation.class, MysqlInput.class, MysqlOutput.class),
    ORACLE("oracle", OracleInputLocation.class, OracleOutputLocation.class, OraclelInput.class, OracleOutput.class),
    //	DTS("dts", DtsInputLocation.class, null, DtsInput.class, null),
    Sqlserver("sqlserver", SqlserverInputLocation.class, null, SqlserverInput.class, null),
    NOOUTPUT("nooutput", null, DummyLocation.class, null, DummyOutput.class),
    JDBC("jdbc", JDBCLocation.class, JDBCOutputLocation.class, JDBCInput.class, JDBCOutput.class),
    ES("es", null, ESOutputLocation.class, null, ESOutput.class),
    EXCEL("excel",null,EXCELOutputLocation.class,null,ExcelOutput.class),
    HBASE("hbase",HbaseInputputLocation.class,HbaseOutputLocation.class,HbaseInput.class,HbaseOutput.class);

//	MONGO("mongo", null, MongoOutputLocation.class, null, MongoDBOutput.class);


    public final String type;

    public final Class<? extends LocationConfig> inputLocationClass;
    public final Class<? extends LocationConfig> outputLocationClass;
    public final Class<? extends Input> inputClass;
    public final Class<? extends Output> outputClass;

    ConfigTypeVals(String type, Class<? extends LocationConfig> locationinputClass,
                   Class<? extends LocationConfig> locationoutputClass,
                   Class<? extends Input> inputClass, Class<? extends Output> outputClass) {
        this.type = type;
        this.inputLocationClass = locationinputClass;
        this.outputLocationClass = locationoutputClass;
        this.inputClass = inputClass;
        this.outputClass = outputClass;
    }

    public static ConfigTypeVals getByType(String type) {

        if (type == null) {
            return null;
        }
        for (ConfigTypeVals c : values()) {
            if (type.equals(c.type)) {
                return c;
            }
        }
        return null;
    }

    public static ConfigTypeVals getByInputLocation(Class<? extends LocationConfig> inputLocation) {

        if (inputLocation == null) {
            return null;
        }
        for (ConfigTypeVals c : values()) {
            if (inputLocation.equals(c.inputLocationClass)) {
                return c;
            }
        }
        return null;
    }

    public static ConfigTypeVals getByOutputLocation(Class<? extends LocationConfig> outputLocation) {

        if (outputLocation == null) {
            return null;
        }
        for (ConfigTypeVals c : values()) {
            if (outputLocation.equals(c.outputLocationClass)) {
                return c;
            }
        }
        return null;
    }

    public static ConfigTypeVals getByInput(Class<? extends Input> input) {

        if (input == null) {
            return null;
        }
        for (ConfigTypeVals c : values()) {
            if (input.equals(c.inputClass)) {
                return c;
            }
        }
        return null;
    }

    public static ConfigTypeVals getByOutput(Class<? extends Output> output) {

        if (output == null) {
            return null;
        }
        for (ConfigTypeVals c : values()) {
            if (output.equals(c.outputClass)) {
                return c;
            }
        }
        return null;
    }
}