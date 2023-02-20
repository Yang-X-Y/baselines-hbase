package tiny.mdhbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.spark.api.java.JavaPairRDD;

import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @Author zhangjianhao
 * @Date 2021/5/22
 */
public class BulkloadUtil {

    /**
     * 将封装好的RDD批量导入至HBase
     *
     * @param hFileRDD 封装好的HFileRDD
     * @param conf     配置文件
     * @throws Exception
     */
    public static void bulkLoad(JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD,
                                Configuration conf, String tableName) throws Exception {
        //生成临时地址
        String outputPath = "/tmp-bulkload/" + ThreadLocalRandom.current().nextDouble();

        final FileSystem fileSystem = FileSystem.get(new URI(outputPath), conf);
        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }

        //保存到指定目录
        hFileRDD.saveAsNewAPIHadoopFile(outputPath,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                conf);

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName));
             Admin admin = connection.getAdmin()) {

            //获取hbase表的region分布
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));

            //通过它进行落地存储 将临时存储在hdfs上的hfile导入到hbase中
            LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(conf);
            bulkLoader.doBulkLoad(new Path(outputPath), admin, table, regionLocator);
        }

        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }

    }
}
