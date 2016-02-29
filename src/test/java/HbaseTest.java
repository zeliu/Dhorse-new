import cn.wanda.dataserv.config.resource.HbaseConf;
import cn.wanda.dataserv.config.resource.HbaseServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created by liuze on 2016/1/25 0025.
 */
public class HbaseTest {

    public static void main(String args[]){

        /*Date date=new Date();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(calendar.DATE,-1);//把日期往后增加一天.整数往后推,负数往前移动
        date=calendar.getTime(); //这个时间就是日期往后推一天的结果
        String yesterday=(new Date().getTime()-24*60*60*1000)/1000+"";
        System.out.println(yesterday);
        System.out.println(date.getTime()/1000);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 0);
        String yesterday1 = new SimpleDateFormat( "yyyy-MM-dd 00:00:00").format(cal.getTime());
        System.out.println(yesterday1);
        System.out.println(getDay("2016-01-28"));
        SimpleDateFormat sdf =   new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date1 = sdf.parse(yesterday1+" 00:00:00");
            yesterday=(date1.getTime())/1000+"";
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(yesterday);*/
        HTable table = getHTable("message_push_message");
        Scan scan = new Scan();
        //Filter filter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("sendTime"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(yesterday));
        //scan.setFilter(filter);
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            byte[] mobileByte = result.getValue(Bytes.toBytes("base_info"),Bytes.toBytes("sendtime"));
            System.out.println(Bytes.toString(mobileByte));
            System.out.println(Bytes.toString(result.getRow()));

        }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println((new Date().getTime()-24*60*60*1000)/1000);

/*        String aaa="张三\r李四\n王五" ;
        System.out.println(aaa);
        System.out.println(aaa.replace("\n","").replace("\r",""));*/
    }


    private static HTable getHTable(String tableName) {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.security.authorization", "flase");
        conf.set("zookeeper.znode.parent", "/hbase-wanda");
        conf.set("hbase.zookeeper.quorum", "CDM1C22-209021017.wdds.com,CDM1C22-209021016.wdds.com,CDM1C22-209021015.wdds.com,CDM1C22-209021014.wdds.com,CDM1C22-209021018.wdds.com");
        conf.set("hbase.zookeeper.property.clientPort", "10218");
        //conf.set("zookeeper.znode.parent", "/hbase-uhp");
        //conf.set("hbase.zookeeper.quorum", "CDM1C02-209018033.wdds.com,CDM1C02-209018032.wdds.com,CDM1C02-209018034.wdds.com");
        //conf.set("hbase.zookeeper.property.clientPort", "10218");
        TableName objTableName = TableName.valueOf(tableName);
        HTable objHtable =null;
        try {
            objHtable = new HTable(conf, objTableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return objHtable;
    }


    private static String getDay(String date){

        String day="9999999999";
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
