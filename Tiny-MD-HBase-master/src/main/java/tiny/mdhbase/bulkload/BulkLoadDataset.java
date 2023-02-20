package tiny.mdhbase.bulkload;

import com.google.common.collect.Iterators;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import tiny.mdhbase.Point;
import tiny.mdhbase.util.BulkloadUtil;
import tiny.mdhbase.util.BytesArray;
import tiny.mdhbase.util.Utils;
import java.io.Serializable;

public class BulkLoadDataset implements Serializable {

    public void bulkLoadToPointsTable(Configuration conf, String tableNameStr, JavaRDD<Pair<String,Geometry>> geometryRDD) throws Exception{

        int PRECISION = 1000000;

        JavaRDD<KeyValue> kvRDD = geometryRDD
                .mapPartitions(iterator -> Iterators.transform(iterator, girdGeom -> {
                long id = Long.parseLong(girdGeom.getKey());
                Geometry geom = girdGeom.getValue();

                Coordinate coordinate = geom.getCoordinate();
                int integerLng = (int) ((coordinate.x+180.0) * PRECISION);
                int integerLat = (int) ((coordinate.y+90.0) * PRECISION);
                Point p = new Point(id, integerLng, integerLat);
                byte[] rowKey = Utils.bitwiseZip(p.x, p.y);

                byte[] bx = Bytes.toBytes(p.x);
                byte[] by = Bytes.toBytes(p.y);

                byte[] value = Utils.concat(bx, by);

                KeyValue geomKV = new KeyValue(rowKey,
                        Bytes.toBytes("P"),
                        Bytes.toBytes(p.id),
                        value);
                return geomKV;
                }));

        JavaRDD<KeyValue> sortedKvRDD = kvRDD.sortBy(kv ->
                        new BytesArray(new byte[][]{
                                CellUtil.cloneRow(kv),
                                CellUtil.cloneQualifier(kv)}),
                true, kvRDD.getNumPartitions());

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD = sortedKvRDD.mapToPair(kv ->
                new Tuple2<>(new ImmutableBytesWritable(CellUtil.cloneRow(kv)), kv));

        System.out.println("*************3:bulkloading Points size:"+hFileRDD.count());
        BulkloadUtil.bulkLoad(hFileRDD, conf, tableNameStr);

    }

}
