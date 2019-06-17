package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Hbase操作测试
 **/
public class OptionTest {
    private Admin admin = null;
    private Connection connection = null;
    private Configuration conf = null;
    private TableName tname = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "datanode01,datanode02,datanode03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "namenode:60000");

        connection = ConnectionFactory.createConnection(conf);

        admin = connection.getAdmin();
    }

    //创建一张表，指定表名，列族
    @Test
    public void createTabl() throws IOException {

        try {
            admin.createNamespace(NamespaceDescriptor.create("MyNamespace").build());
        } catch (NamespaceExistException e) {
            System.out.println("该命名空间已经存在");
        }

        //创建tablename对象,描述表的名称信息
        tname = TableName.valueOf("MyNamespace:students");
        //创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        if (admin.tableExists(tname)) {
            System.out.println("students" + "存在！");
            System.exit(0);
        } else {
            HColumnDescriptor famliy = new HColumnDescriptor("core");
            tDescriptor.addFamily(famliy);
            admin.createTable(tDescriptor);
            System.out.println("创建表成功！");
        }
    }

    //获取所有的表
    @Test
    public void getAllTable() {
        if (admin != null) {
            try {
                HTableDescriptor[] alltable = admin.listTables();
                for (HTableDescriptor hTableDesc : alltable) {
                    System.out.println(hTableDesc.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //删除表
    @Test
    public void deleteTab() throws IOException {
        tname = TableName.valueOf("MyNamespace:students");
        if (admin.tableExists(tname)) {
            admin.disableTable(tname);//先禁用表才能删除
            admin.deleteTable(tname);
            System.out.println("删除表成功！");
        } else {
            System.out.println("表不存在");
        }
    }

    //插入一条数据
    @Test
    public void addOneRecord() throws IOException {

        //通过连接查询tableName对象
        tname = TableName.valueOf("MyNamespace:students");

        if(admin.tableExists(tname)) {

            Table table = connection.getTable(tname);

            Put put = new Put(Bytes.toBytes("lisi"));
            put.add(Bytes.toBytes("core"), Bytes.toBytes("math"), Bytes.toBytes("98"));

            table.put(put);
            System.out.println("插入数据成功！");
        }else {
            System.out.println("插入数据失败");
        }
    }

    //插入多条数据
    @Test
    public void moreInsert() throws IOException {
        //测试在数据前补零
        DecimalFormat format = new DecimalFormat();
        format.applyPattern("0000");

        tname = TableName.valueOf("MyNamespace:students");

        HTable htable = (HTable) connection.getTable(tname);

        //不要自动清理缓冲区
        htable.setAutoFlush(false);

        // 一个put代表一行数据，再new一个put表示第二行数据,每行一个唯一的RowKey
        for (int i = 1; i < 10000; i++) {
            Put put = new Put(Bytes.toBytes("leilei" + format.format(i)));
            //关闭写前日志
            put.setWriteToWAL(false);


            put.addColumn(Bytes.toBytes("core"), Bytes.toBytes("math"), Bytes.toBytes(format.format(i)));
            put.addColumn(Bytes.toBytes("core"), Bytes.toBytes("english"), Bytes.toBytes(format.format(Math.random() * i)));
            put.addColumn(Bytes.toBytes("core"), Bytes.toBytes("chinese"), Bytes.toBytes(format.format(Math.random() * i)));
            htable.put(put);

            if (i % 2000 == 0) {
                htable.flushCommits();
            }
        }
        htable.flushCommits();
        htable.close();
    }


    //通过RowKey,faimly,colum获取cell数据
    @Test
    public void getData() throws IOException {
        tname = TableName.valueOf("MyNamespace:students");

        Table table = connection.getTable(tname);

        //通过RowKey
        Get get = new Get(Bytes.toBytes("lisi"));

        Result result = table.get(get);

        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("core"), Bytes.toBytes("math"))));
    }

    //扫描在rowkey范围内的cell数据
    @Test
    public void deleteRangeRK() throws IOException {
        tname = TableName.valueOf("MyNamespace:students");
        Table table = connection.getTable(tname);

        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("leilei1000"));
        scan.setStopRow(Bytes.toBytes("leilei9999"));


        ResultScanner resultScanner = table.getScanner(scan);

        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            System.out.println(Bytes.toString((iterator.next()).getValue(Bytes.toBytes("core"), Bytes.toBytes("english"))));
//            System.out.println((iterator.next()).toString());
        }
    }

    //KeyValue形式查询一行的数据
    @Test
    public void getValueFromKey() throws IOException {
        tname = TableName.valueOf("MyNamespace:students");
        Table table =  connection.getTable(tname);
        Get get = new Get(Bytes.toBytes("leilei1000"));

        Result result = table.get(get);
        if (result.raw().length == 0) {
            System.out.println("不存在该关键字的行！!");

        } else {
            for (Cell kv : result.rawCells()) {
                System.out.println(
                        "列:"+Bytes.toString(CellUtil.cloneFamily(kv))+":"+Bytes.toString(CellUtil.cloneQualifier(kv))
                                +"\t 值:"+Bytes.toString(CellUtil.cloneValue(kv)));
            }

        }
    }

    //KeyValue形式查询所有的数据
    @Test
    public void getAllData() throws Exception {
        tname = TableName.valueOf("MyNamespace:students");
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

    // 删除一行Hbase表中记录信息的
    @Test
    public void deleteRecord() throws IOException {
        tname = TableName.valueOf("MyNamespace:students");
        Table table = connection.getTable(tname);
        Delete de = new Delete(Bytes.toBytes("leilei9997"));
        try {
            table.delete(de);
            System.out.println("删除记录成功！！！");
        } catch (IOException e) {
            System.out.println("删除记录异常！！！");

        }
    }

    @After
    public void destory() throws IOException {
        admin.close();
        connection.close();
    }
}
