/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package tiny.mdhbase;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;
import tiny.mdhbase.util.JsonUtil;
import tiny.mdhbase.util.Utils;
import java.io.*;
import java.util.*;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Tiny MD-HBase Client
 * 
 * @author shoji
 * 
 */
public class myClient implements Closeable {

  private final Index index;
  private final WKTReader wktReader;
  private final int PRECISION = 10000000;

  public myClient(String tableName, int splitThreshold) throws IOException {
    this.wktReader = new WKTReader();
    this.index = new Index(HBaseConfiguration.create(), tableName,
        splitThreshold);
  }


  /**
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    JSONObject jsonConf = JsonUtil.readLocalJSONFile(args[0]);
    String tableName = jsonConf.getString("tableName");
    String inputFile = jsonConf.getString("inputFile");// 导入数据
    int splitThreshold = jsonConf.getIntValue("splitThreshold");// 桶分割阈值
    String pointRecordFile = jsonConf.getString("pointRecordFile");// knn 查询条件
    String rangeRecordFile = jsonConf.getString("rangeRecordFile");// range查询条件
    String operation = jsonConf.getString("operation");// 操作: loadData; range query; knn query
    int numK = jsonConf.getIntValue("numK");// knn的k值

    myClient client = new myClient(tableName, splitThreshold);

    try {
      switch (operation) {
        case "loadData":
          client.loadData(inputFile);
          break;
        case "rangeQuery":
          List<Tuple2<Range, Range>> queryRanges = client.generateQueryBoxByRecords(rangeRecordFile);
          for (Tuple2<Range, Range> range : queryRanges) {
            Iterable<Point> points = client.rangeQuery(range._1, range._2);
            System.out.println(Iterables.size(points));
          }
          break;
        case "knnQuery":
          List<Point> points = client.generateQueryPointsByRecords(pointRecordFile);
          for (Point queryPoint : points) {
            Iterable<Point> knnResult = client.nearestNeighbor(queryPoint, numK);
            for (Point p : knnResult) {
              System.out.println(p.toString()+"\t distance:"+p.distanceFrom(queryPoint));
            }
          }
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      Closeables.closeQuietly(client);
    }
  }

  public List<Tuple2<Range, Range>> generateQueryBoxByRecords(String recordFile) {

    List<Tuple2<Range, Range>> queryBoxes = new ArrayList<Tuple2<Range, Range>>();
    try {
      // create a reader instance
      BufferedReader br = new BufferedReader(new FileReader(recordFile));
      // read until end of file
      String line;
      while ((line = br.readLine()) != null) {
        JSONObject jsonObj = JSONObject.parseObject(line);
        if (jsonObj.containsKey("minLng")) {
          int minLng = (int) ((jsonObj.getDoubleValue("minLng")+180)*PRECISION);
          int minLat = (int) ((jsonObj.getDoubleValue("minLat")+90)*PRECISION);
          int maxLng = (int) ((jsonObj.getDoubleValue("maxLng")+180)*PRECISION);
          int maxLat = (int) ((jsonObj.getDoubleValue("maxLat")+90)*PRECISION);
          Range xRange = new Range(minLng, maxLng);
          Range yRange = new Range(minLat, maxLat);
          Tuple2<Range, Range> range = new Tuple2<Range,Range>(xRange,yRange);
          queryBoxes.add(range);
        }
      }

      // close the reader
      br.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }
    System.out.println("generateQueryRangesSize: "+queryBoxes.size());
    return queryBoxes;
  }


  public List<Point> generateQueryPointsByRecords(String recordFile) {
    List<Point> queryPoints = new ArrayList<Point>();
    try {
      // create a reader instance
      BufferedReader br = new BufferedReader(new FileReader(recordFile));
      // read until end of file
      String line;
      long queryPointId = 0;
      while ((line = br.readLine()) != null) {
        JSONObject jsonObj = JSONObject.parseObject(line);
        if (jsonObj.containsKey("pointLng")) {
          // MD HBase 只支持正数；
          int pointLng = (int) ((jsonObj.getDoubleValue("pointLng")+180)*PRECISION);
          int pointLat = (int) ((jsonObj.getDoubleValue("pointLat")+90)*PRECISION);
          queryPointId+=1;
          Point point = new Point(queryPointId, pointLng, pointLat);
          queryPoints.add(point);
        }
      }

      // close the reader
      br.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return queryPoints;
  }

  public void loadData(String inputFile) throws Exception {

    System.out.println("write data");
    long records = 0;
    long id = 0;
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
    String line;
    while ((line = bufferedReader.readLine())!= null){
      String[] strings = line.split("@");
//        String id = strings[0];
      String geomWKT = strings[1];
      Coordinate coordinate = wktReader.read(geomWKT).getCoordinate();

      int integerLng = (int) ((coordinate.x+180) * PRECISION);
      int integerLat = (int) ((coordinate.y+90) * PRECISION);
      id+=1;
      Point p = new Point(id, integerLng, integerLat);
      byte[] row = Utils.bitwiseZip(p.x, p.y);
      Bucket bucket = index.fetchBucket(row);
      bucket.insertMain(row, p);
      bucket.insertIndex(row, p);
      if (++records % 100000 == 0){
        System.out.println("当前导入点数：" + records);
        System.out.println("当前桶分割次数：" + index.getSplitTimes());
      }
    }
    System.out.println("导入完成,总点数 ：" + records);
    System.out.println("桶分割总次数 ：" + index.getSplitTimes());
    System.out.println("idCount ：" + id);
  }

  /**
   * 
   * @param rx
   *          a query range on dimension x
   * @param ry
   *          a query range on dimension y
   * @return points within the query region
   * @throws IOException
   */
  public Iterable<Point> rangeQuery(Range rx, Range ry) throws IOException {
    Iterable<Bucket> buckets = index.findBucketsInRange(rx, ry);
    List<Point> results = new LinkedList<Point>();
    for (Bucket bucket : buckets) {
      results.addAll(bucket.scan(rx, ry));
    }
    return results;
  }

  /**
   * 
   * @param point
   * @param k
   * @return
   * @throws IOException
   */
  public Iterable<Point> nearestNeighbor(final Point point, int k)
      throws IOException {
    NavigableSet<Point> results = new TreeSet<Point>(new Comparator<Point>() {

      @Override
      public int compare(Point o1, Point o2) {
        return Double.compare(point.distanceFrom(o1), point.distanceFrom(o2));
      }

    });

    PriorityQueue<Bucket> queue = new PriorityQueue<Bucket>(Integer.MAX_VALUE,
        new Comparator<Bucket>() {

          @Override
          public int compare(Bucket o1, Bucket o2) {
            return Double.compare(o1.distanceFrom(point),
                o2.distanceFrom(point));
          }

        });

    double farthest = Double.MAX_VALUE;
    int offset = 0;
    Set<String> scannedBucketNames = new HashSet<String>();
    do {
      Iterable<Bucket> buckets = index.findBucketsInRange(new Range(point.x
          - offset, point.x + offset), new Range(point.y - offset, point.y
          + offset));

      for (Bucket bucket : buckets) {
        if (!scannedBucketNames.contains(bucket.toString())) {
          queue.add(bucket);
        }
      }
      if (queue.size() == 0) {
        return results; // there are no buckets to be scanned.
      }

      for (Bucket bucket = queue.poll(); bucket != null; bucket = queue.poll()) {
        if (bucket.distanceFrom(point) > farthest) {
          return results;
        }

        for (Point p : bucket.scan()) {
          if (results.size() < k) {
            results.add(p); // results.size() is at most k.
          } else {
            results.add(p);
            results.pollLast(); // remove the (k+1)-th point.
            farthest = results.last().distanceFrom(point);
          }
        }
        scannedBucketNames.add(bucket.toString());
        int newOffset = (int) Math.ceil(Math.sqrt(point.distanceFrom(bucket
            .farthestCornerFrom(point))));
        offset = Math.max(offset, newOffset);
      }
    } while (true);

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    index.close();
  }

}
