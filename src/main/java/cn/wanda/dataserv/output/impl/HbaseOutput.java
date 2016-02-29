package cn.wanda.dataserv.output.impl;

/**
 * Created by liuze on 2015/10/29 0029.
 */

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.HbaseOutputLocation;
import org.apache.hadoop.hbase.HBaseConfiguration;
import cn.wanda.dataserv.config.resource.HbaseConf;
import cn.wanda.dataserv.config.resource.HbaseServer;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.AbstractOutput;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;


import java.io.*;
import java.util.ArrayList;
import java.util.List;


@Log4j
public class HbaseOutput extends AbstractOutput {

    HbaseConf hbaseConf;
    HbaseServer hbaseServer;

    HbaseOutputLocation location;



    protected String table;

    protected String family;

    protected String columns;

    protected String rowkeyDelim;


    protected String rowkeyLocation;

    protected String valueLocation;


    protected Configuration conf;

    protected HTable hTable;

    protected List<Put> listput=new ArrayList<Put>();


    @Override
    public void writeLine(Line line) {
        String str = line.getLine();
        String[] arr=str.split("\001");
        String[] rowkeyarr=this.rowkeyLocation.split(",");
        String[] valuearr=this.valueLocation.split(",");
        String[] columnarr=this.columns.split(",");
        String rowkey="";
        for(int i=0;i<rowkeyarr.length;i++){
            int j=Integer.parseInt(rowkeyarr[i]);
            rowkey=rowkey+this.rowkeyDelim+arr[j-1];

        }
        Put objPut = new Put(Bytes.toBytes(rowkey.substring(1, rowkey.length())));
        for(int i=0;i<columnarr.length;i++){
            int j=Integer.parseInt(valuearr[i]);
            String qualifierName=columnarr[i];
            String qualifierVal =arr[j-1];
            objPut.add(Bytes.toBytes(this.family),
                    Bytes.toBytes(qualifierName),
                    Bytes.toBytes(qualifierVal));
        }
        listput.add(objPut);
        try {
            if(listput.size()==1000) {
                hTable.put(listput);
                listput.clear();
            }
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }


    }
    @Override
    public void last(boolean success) {
        if(success&&listput.size()!=0){
            try {
                hTable.put(listput);
            } catch (InterruptedIOException e) {
                e.printStackTrace();
            } catch (RetriesExhaustedWithDetailsException e) {
                e.printStackTrace();
            }
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

    }

    @Override
    public void init() {
        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!HbaseOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not mysql!");
        }

        location = (HbaseOutputLocation) l;

        hbaseConf = location.getHbaseConf();
        hbaseServer = location.getHbaseServer();

        this.hTable = getHTable(hbaseConf,hbaseServer);

        this.table = hbaseConf.getTable();
        this.family = hbaseConf.getFamily();
        this.columns = hbaseConf.getColumns();
        this.rowkeyDelim = hbaseConf.getRowkeyDelim();
        this.rowkeyLocation = hbaseConf.getRowkeyLocation();
        this.valueLocation = hbaseConf.getValueLocation();
    }




    @Override
    public void prepare() {

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






}

