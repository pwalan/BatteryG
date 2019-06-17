package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class HBaseUtils {
    private static Admin admin = null;
    private static Connection connection = null;
    private static Configuration conf = null;
    private static TableName tname = null;

    public static void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "datanode01,datanode02,datanode03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "namenode:60000");

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    public static void createNamespace(String namespace) throws IOException {
        try {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        } catch (NamespaceExistException e) {
            System.out.println("该命名空间已经存在");
        }
    }

    public static void createTable(String namespace, String tableName, String famliyName) throws IOException {
        init();
        //创建tablename对象,描述表的名称信息
        tname = TableName.valueOf(namespace + ":" + tableName);
        //创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        if (admin.tableExists(tname)) {
            System.out.println("表" + tableName + "已存在！");
            System.exit(0);
        } else {
            HColumnDescriptor famliy = new HColumnDescriptor(famliyName);
            tDescriptor.addFamily(famliy);
            admin.createTable(tDescriptor);
            System.out.println("创建表成功！");
        }
    }

    public static void insertFromOneFile(String namespace, String tableName, String filePath) throws IOException {
        init();

        tname = TableName.valueOf(namespace + ":" + tableName);

        HTable htable = (HTable) connection.getTable(tname);

        //不要自动清理缓冲区
        htable.setAutoFlush(false);

        File file = new File(filePath);
        InputStreamReader in_stream = new InputStreamReader(new FileInputStream(file));
        BufferedReader in = new BufferedReader(in_stream);
        String s;
        int i = 0;

        while ((s = in.readLine()) != null) {

            String[] items = s.split(",");
            // 一个put代表一行数据，再new一个put表示第二行数据,每行一个唯一的RowKey
            Put put = new Put((file.getName() + items[1].split(":")[1]).getBytes());
            //关闭写前日志
            put.setWriteToWAL(false);
            for (String item : items) {
                String[] kv = item.split(":");
                if (kv[0].equals("VID")) {
                    put.addColumn(Bytes.toBytes("core"), Bytes.toBytes("VID"), Bytes.toBytes(file.getName()));
                } else if (kv.length == 2) {
                    put.addColumn(Bytes.toBytes("core"), Bytes.toBytes(kv[0]), Bytes.toBytes(kv[1]));
                }
            }
            htable.put(put);
            if (i++ % 2000 == 0) {
                htable.flushCommits();
            }
        }
        htable.flushCommits();
        htable.close();
    }

    public static void insertFromDirectory(String namespace, String tableName, String dirPath) throws IOException {

    }


    public static void getAllData(String namespace, String tableName) throws Exception {
        init();
        tname = TableName.valueOf(namespace + ":" + tableName);
        Table table = connection.getTable(tname);

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.println(Bytes.toString(kv.getKey())
                        + Bytes.toString(kv.getValue()));
            }
        }
    }

    public static void getValueFromKey(String namespace, String tableName, String rowKey) throws IOException {
        init();
        tname = TableName.valueOf(namespace + ":" + tableName);
        Table table = connection.getTable(tname);
        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);
        if (result.raw().length == 0) {
            System.out.println("不存在该关键字的行！!");

        } else {
            for (Cell kv : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(kv)) + ":" + Bytes.toString(CellUtil.cloneQualifier(kv))+",");
                /*System.out.println(
                        "列:" + Bytes.toString(CellUtil.cloneFamily(kv)) + ":" + Bytes.toString(CellUtil.cloneQualifier(kv))
                                + "\t 值:" + Bytes.toString(CellUtil.cloneValue(kv)));*/
            }

        }
    }

    public static void main(String[] args) throws Exception {
        //createTable("MyNamespace", "test", "core");

        String filePath = "/Users/alanp/Downloads/abcdefg";
        //insertFromOneFile("MyNamespace", "test", filePath);

        //getAllData("MyNamespace", "test");

        getValueFromKey("MyNamespace", "test", "abcdefg20160304154448");
    }
}
