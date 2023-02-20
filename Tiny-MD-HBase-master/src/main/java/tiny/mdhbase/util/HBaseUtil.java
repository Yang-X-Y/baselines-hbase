package tiny.mdhbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class HBaseUtil {

    //public static Configuration configuration = null;
    //public static Connection connection = null;
    //public static Admin admin = null;
    private static Logger log = Logger.getLogger("HBaseUtil");

    public static Connection initConnection(){
        Configuration configuration = HBaseConfiguration.create();
        /*虚拟机HBase
        configuration.set("hbase.zookeeper.quorum","master");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");

        */


        //公司Hbase
        //configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
        /*
        configuration.set("hbase.rootdir", "hdfs://master:8020/HBase_DB");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        */

        //hlx Hbase
        /*configuration.set("hbase.rootdir", "hdfs://master:9000/HBase_DB");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");*/



        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            log.info("连接成功！");
            return connection;
        }catch (Exception e){
            log.info("连接失败！");
            e.toString();
            return null;
        }
    }

   /* public static void close(){
        try{
            if(admin!=null)
                admin.close();
            if(connection!=null)
                connection.close();
        }catch (Exception e){
            e.printStackTrace();
            log.info("关闭失败");
        }
    }*/

    public static void createTable(Connection connection, String myTableName,String[] colFamilys){

        try{
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(myTableName);
            if(admin.tableExists(tableName)){
                log.info(String.format("创建表：%s,表已存在！", myTableName));
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                log.info(String.format("删除表：%s;", myTableName));
            }
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(myTableName));
            for (String familyName:colFamilys){
                hbaseTable.addFamily(new HColumnDescriptor(familyName));
            }
            admin.createTable(hbaseTable);
            log.info("创建表成功");
//            else{
//                /* 2.0 API
//                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);//2.x API
//                for(String colFamily:colFamilys){
//                    tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build());
//                }
//                admin.createTable(tableDescriptorBuilder.build());
//
//                 */
//                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(myTableName));
//                for (String familyName:colFamilys){
//                    hbaseTable.addFamily(new HColumnDescriptor(familyName));
//                }
//                admin.createTable(hbaseTable);
//                log.info("创建表成功");
//            }

            /*
            1.x API
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                 HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                for (String familyName:familyNames){
                     hbaseTable.addFamily(new HColumnDescriptor(familyName));
                 }
                admin.createTable(hbaseTable);
             }
             */

        }catch (Exception e){
            log.severe(e.toString());
            log.info("创建表失败");
        }
    }

    public static void deleteTable(Connection connection, String tableName){

        try{
            Admin admin = connection.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            if(admin.tableExists(tn)){
                admin.disableTable(tn);
                admin.deleteTable(tn);
                log.info("删除表成功");
            }else{
                log.info("删除表：表不存在");
            }
        }catch (Exception e){
            log.info("删除表失败");
            log.severe(e.toString());
        }
    }

    public static void listAllTables(Connection connection){

        try{
            Admin admin = connection.getAdmin();
            /*
            2.x API
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for(TableDescriptor tableDescriptor:tableDescriptors){
                System.out.println(tableDescriptor.getTableName());
            }
             */
            //1.0 API
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for(HTableDescriptor hTableDescriptor:hTableDescriptors){
                System.out.println(hTableDescriptor.getTableName());
            }

        }catch (Exception e){
            log.severe(e.toString());
            log.severe("列出全部表失败");
        }
    }

    public static void insertRow(Connection connection, String tableName,String rowKey,String colFamily,String col,String val){

        insertRow(connection, tableName,Bytes.toBytes(rowKey), Bytes.toBytes(colFamily),Bytes.toBytes(col),Bytes.toBytes(val));
    }

    public static void insertRow(Connection connection, String tableName,String rowKey,String colFamily,String col,byte[] val){

        insertRow(connection, tableName,Bytes.toBytes(rowKey), Bytes.toBytes(colFamily),Bytes.toBytes(col),val);
    }

    public static void insertRow(Connection connection, String tableName,byte[] rowKey,
                                 byte[] colFamily,byte[] col,byte[] val){
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(rowKey);
            put.addColumn(colFamily,col,val);
            table.put(put);
            table.close();
            log.info("插入成功");
        }catch (Exception e){
            log.severe(e.toString());
            log.info("插入失败");
        }
    }

    public static  void deleteRow(Connection connection, String tableName,String rowKey,String colFamily,String col){
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            table.close();
            log.info("删除表成功");
        }catch (Exception e){
            log.severe(e.toString());
            log.info("删除失败");
        }
    }

    public static byte[] getData(Connection connection, String tableName,String rowKey,String colFamily,String col){
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            Result result = table.get(get);
            byte[] bytes = result.getValue(colFamily.getBytes(),col.getBytes());
            table.close();
            //log.info("获取数据成功");
            return bytes;
        }catch (Exception e){
            log.severe(e.toString());
            log.info("获取数据失败");
            return null;
        }
    }

    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(int i=0;i<cells.length;i++){
            Cell cell = cells[i];
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell)));
            System.out.println("Timetamp:"+new String(cell.getTimestamp()+""));
            System.out.println("colFamily:"+new String(CellUtil.cloneFamily(cell)));
            System.out.println("col Name:"+new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value:"+new String(CellUtil.cloneValue(cell)));
        }
    }

    public static void batch(Connection connection, String myTableName, List<Row> actions){
        Object[] results = new Object[actions.size()];
        try{
            TableName tableName = TableName.valueOf(myTableName);
            Table table = connection.getTable(tableName);
            table.batch(actions,results);

        }catch (Exception e ){
            log.severe(e.toString());
            log.info("批量操作失败");
        }

        for(Object result : results){
            System.out.println(result.toString());
        }


    }

    public static void scan(Connection connection, String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result:resultScanner){
                System.out.println(new String(result.getRow()));
            }
            resultScanner.close();
        }catch (IOException e){
            log.severe(e.toString());
            log.info("扫描错误");
        }

    }


    public static void loadCoprocessor(Connection connection, String tableName,Class coprocessorClass,String coprocessorPath){
        try {
            Admin admin = connection.getAdmin();
            Table table = connection.getTable(TableName.valueOf(tableName));
            admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            Path path = new Path(coprocessorPath);

            hTableDescriptor.addCoprocessor(coprocessorClass.getCanonicalName(),path, Coprocessor.PRIORITY_USER,null);
            admin.modifyTable(TableName.valueOf(tableName),hTableDescriptor);
            admin.enableTable(TableName.valueOf(tableName));

            log.info("加载协处理器成功!");
        }catch (Exception e){
            log.severe("加载协处理器失败！");
        }

    }

    public static void unloadCoprocessor(Connection connection, String tableName,Class coprocessorClass){
        try {
            Admin admin = connection.getAdmin();
            Table table = connection.getTable(TableName.valueOf(tableName));
            admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            hTableDescriptor.removeCoprocessor(coprocessorClass.getCanonicalName());
            admin.modifyTable(TableName.valueOf(tableName),hTableDescriptor);
            admin.enableTable(TableName.valueOf(tableName));
            log.info("卸载协处理器成功");
        }catch (Exception e){
            log.severe("卸载协处理器失败！");
        }

    }


    public static void deleteColumnData(Table table,String colFamily,String colName){
        try {
            Scan scan = new Scan();
            scan.addColumn(colFamily.getBytes(),colName.getBytes());
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();
            ArrayList<Delete> deletes = new ArrayList<>();
            while (iterator.hasNext()){
                Delete delete = new Delete(iterator.next().getRow());
                delete.addColumn(colFamily.getBytes(),colName.getBytes());
                deletes.add(delete);
            }
            table.delete(deletes);

        }catch (Exception e){
            log.severe(e.toString());
        }
    }

    public static List<Pair<byte[], byte[]>> getRegionRanges(Connection connection, String tableName){
        try {
            Admin admin = connection.getAdmin();
            List<HRegionInfo> hRegionInfos = admin.getTableRegions(TableName.valueOf(tableName));
            List<Pair<byte[], byte[]>> regionRanges = new ArrayList<>();
            for(HRegionInfo hRegionInfo : hRegionInfos){
                regionRanges.add(new Pair<>(hRegionInfo.getStartKey(), hRegionInfo.getEndKey()));
            }
            log.info("获取region范围成功");
            return regionRanges;
        }catch (IOException e){
            log.info("获取region范围失败");
            e.printStackTrace();
            return null;
        }
    }

    public static List<byte[]> getSampleRowKeyByFraction(Connection connection, String tableName, float chance){
        try {
            Random random = new Random();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            FilterList filterList = new FilterList();
            filterList.addFilter(new KeyOnlyFilter());
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            List<byte[]> res = new ArrayList<>();
            Iterator<Result> iterator = resultScanner.iterator();
            res.add(iterator.next().getRow());//add startKey

            while (iterator.hasNext()){
                byte[] bytes = iterator.next().getRow();
                if (!iterator.hasNext()  //add endKey
                        || random.nextFloat() < chance){
                    res.add(bytes);
                }
            }
            resultScanner.close();
            return res;
        }catch (IOException e){
            log.severe(e.toString());
            log.info("扫描错误");
            return null;
        }
    }


}
