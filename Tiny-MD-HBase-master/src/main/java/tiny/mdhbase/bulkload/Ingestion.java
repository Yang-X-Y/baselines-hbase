package tiny.mdhbase.bulkload;

import com.alibaba.fastjson.JSONObject;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import tiny.mdhbase.util.HBaseUtil;
import tiny.mdhbase.util.JsonUtil;
import java.util.Objects;


public class Ingestion {

    public static void main(String[] args) throws Exception{

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        Configuration configuration = HBaseConfiguration.create();

        JSONObject jsonObject = JsonUtil.readLocalJSONFile(args[0]);
        Broadcast<JSONObject> argsJson = javaSparkContext.broadcast(jsonObject);

        String inputPath = argsJson.getValue().getString("inputFile");//输入文件
        String mainIndexTable = argsJson.getValue().getString("tableName");//主索引表
        String secondaryIndexTable = mainIndexTable+"_index";//二级索引表
        // 数据格式：objectID@WKTGeomtry
        JavaRDD<Pair<String,Geometry>> geometryRDD = javaSparkContext.textFile(inputPath).repartition(120).mapPartitions(new readGeometry()).filter(Objects::nonNull);
        System.out.println("*************1:load geometryRDD size:"+geometryRDD.count());


        BulkLoadDataset bulkLoad = new BulkLoadDataset();
        System.out.println("*************bulkloading...***********");
        try (Connection connection = ConnectionFactory.createConnection(configuration);){
            HBaseUtil.createTable(connection, mainIndexTable, new String[]{"P"});
        }
        bulkLoad.bulkLoadToPointsTable(configuration,mainIndexTable,geometryRDD);
        System.out.println("*************2:bulkloading point MainIndex success");
    }

}
