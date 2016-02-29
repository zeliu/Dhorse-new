package cn.wanda.dataserv.input.impl;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import cn.wanda.dataserv.config.location.HbaseInputputLocation;
import cn.wanda.dataserv.config.resource.HbaseConf;
import cn.wanda.dataserv.config.resource.HbaseServer;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * reminding:<br>
 * use '\t' as field delim in Line read from sqlserver.<br>
 *
 * @author liuze
 */
@Log4j
public class HbaseInput extends AbstractInput implements Input {



    // input params
    private HbaseInputputLocation location;

    private HbaseConf hbaseConf;
    private HbaseServer hbaseServer;

    private String table;

    private String family;

    private String columns;

    private String[] columnarr;
    private String[] familyarr;


    private Scan scan = null;
    private ResultScanner resultScanner = null;

    private Configuration conf;

    private HTable hTable;

    private String fieldsSeparator = "\001";


    @Override
    public void init() {

        // process params
        LocationConfig l = this.inputConfig.getLocation();

        if (!HbaseInputputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not hbase!");
        }

        location = (HbaseInputputLocation) l;

        hbaseConf = location.getHbaseConf();
        hbaseServer = location.getHbaseServer();
        this.table = hbaseConf.getTable();

        this.hTable = getHTable(hbaseConf,hbaseServer);

        this.family = hbaseConf.getFamily();
        this.columns = hbaseConf.getColumns();
        this.columnarr=this.columns.split("\\|");
        this.familyarr=this.family.split("\\|");
        this.scan = new Scan();
        if(hbaseConf.getFilterColumn()!=null&&hbaseConf.getFilterFamily()!=null) {
            List<Filter> filters = new ArrayList<Filter>();
            filters.add(new SingleColumnValueFilter(Bytes.toBytes(hbaseConf.getFilterFamily()), Bytes.toBytes(hbaseConf.getFilterColumn()), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(getDay(hbaseConf.getBeginDate()))));
            filters.add(new SingleColumnValueFilter(Bytes.toBytes(hbaseConf.getFilterFamily()), Bytes.toBytes(hbaseConf.getFilterColumn()), CompareFilter.CompareOp.LESS, Bytes.toBytes(getDay(hbaseConf.getEndDate()))));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);
            scan.setFilter(filterList);
        }
        try {
            this.resultScanner=hTable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Line readLine() {
        try {
        if(resultScanner!=null) {
            StringBuffer sb = new StringBuffer();

            Result result = resultScanner.next();
            if (result == null) {
                return  Line.EOF;

            } else {
                sb.append(Bytes.toString(result.getRow()));
                for(int i=0;i<familyarr.length;i++){
                for (String column : columnarr[i].split(",")) {
                    sb.append(fieldsSeparator + Bytes.toString(result.getValue(Bytes.toBytes(familyarr[i]), Bytes.toBytes(column))).replace("\001",""));
                }
            }


            String line = sb.toString().replace("\n", "").replace("\r","");
            return new Line(line);
        }
        }else{
            return  Line.EOF;
        }
        } catch (IOException e) {
            e.printStackTrace();
            return  Line.EOF;
        }
    }

    @Override
    public void close() {
        if(hTable!=null){
            try {
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if(this.resultScanner!=null){
            resultScanner.close();
        }


    }
    private HTable getHTable(final HbaseConf hbaseConf,final HbaseServer hbaseServer) {

        conf = HBaseConfiguration.create();
        conf.set("hbase.security.authorization", "flase");
        conf.set("zookeeper.znode.parent", hbaseServer.getParent());
        conf.set("hbase.zookeeper.quorum", hbaseServer.getQuorum());
        conf.set("hbase.zookeeper.property.clientPort", hbaseServer.getPort());

        TableName objTableName = TableName.valueOf(hbaseConf.getTable());
        HTable objHtable =null;
        try {
            objHtable = new HTable(conf, objTableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return objHtable;
    }


    private String getDay(String date){

        String day="9999999999";
        if(date==null)
        {
            return day;
        }
        SimpleDateFormat sdf =   new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date1 = sdf.parse(date+" 00:00:00");
            day=(date1.getTime())/1000+"";
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }
}