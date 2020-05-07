package com.haoxin.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/30 12:23
 */
public class HbaseUtil {

    private static Connection conn = null;
    private static Admin admin = null;

    static {
        //创建habse配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://192.168.71.10:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "192.168.71.10");
        conf.set("hbase.client.scanner.timeout.period", "600000");
        conf.set("hbase.rpc.timeout", "600000");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     */
    public static void createTable(String tablename, String[] family) throws IOException {
        TableName tn = TableName.valueOf(tablename);
        if (!admin.tableExists(tn)) {
            HTableDescriptor htd = new HTableDescriptor(tn);
            int length = family.length;
            for (int i = 0; i < length; i++) {
                htd.addFamily(new HColumnDescriptor(family[i]));
            }
            admin.createTable(htd);
            admin.close();
            conn.close();
            System.out.println("表：" + tablename + "创建成功");
        } else {
            System.out.println("表：" + tablename + "已存在");
        }
    }


    /**
     * 删除表
     */
    public static void deleteTable(String tablename) {
        TableName tn = TableName.valueOf(tablename);
        try {
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.println("删除表" + tablename + "成功！");
            } else {
                System.out.println("删除失败，该表已存在！");
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询所有表
     */

    public static TableName[] getTableList() throws Exception {
        TableName[] tableNames = admin.listTableNames();
        for (TableName t : tableNames
        ) {
            System.out.println(t);
        }
        return tableNames;
    }

    /**
     * 查询数据 by rowkey
     */

    public static Result getByRowKey(String tablename, String rowkey) throws IOException {
        TableName tn = TableName.valueOf(tablename);
        boolean tableExists;
        if (admin.tableExists(tn)){
            System.out.println(tn+"表存在");
            Table table = conn.getTable(tn);
            Result result = table.get(new Get(Bytes.toBytes(rowkey)));
            System.out.println("开始查询");
            List<Cell> cells = result.listCells();
            for (Cell cell:cells
                 ) {
                System.out.println("rowkey:"+rowkey);
                System.out.println("family:"+new String(CellUtil.cloneFamily(cell)));
                System.out.println("value:"+new String(CellUtil.cloneValue(cell)));
            }

            return result;
        }
        else {
            System.out.println("表不存在！");
            return null;
        }
    }

    /**
     * 插入数据
     */

    public static void  put(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tablename));
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if (datamap != null){
            Set<Map.Entry<String, String>> entries = datamap.entrySet();
            for (Map.Entry<String, String> e:entries
                 ) {
                String key = e.getKey();
                String value = e.getValue();
                put.addColumn(Bytes.toBytes(famliyname),Bytes.toBytes(key),Bytes.toBytes(value));
            }
        }
        table.put(put);
        table.close();
        System.out.println("ok");
    }

    /**
     * 获取部分数据
     */

    public static  String getdata(String tablename, String rowkey, String famliyname, String colum) throws  Exception{
        Table table = conn.getTable(TableName.valueOf(tablename));
        byte[] rowkeybyte = Bytes.toBytes(rowkey);

        Result result = table.get(new Get(rowkeybyte));
        byte[] value = result.getValue(famliyname.getBytes(), colum.getBytes());
        if (value == null) {
            return null;
        }
        table.close();
        return new String(value);
    }

    /**
     * 插入数据
     */

    public static void putdata(String tablename, String rowkey, String famliyname, String colum, String data) throws  Exception{
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(famliyname.getBytes(),colum.getBytes(),data.getBytes());
        table.put(put);
        table.close();

    }
}
