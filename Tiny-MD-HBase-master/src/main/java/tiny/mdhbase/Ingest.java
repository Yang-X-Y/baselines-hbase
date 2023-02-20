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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;
import tiny.mdhbase.util.JsonUtil;
import tiny.mdhbase.util.Utils;

import java.io.*;
import java.util.*;

/**
 * Tiny MD-HBase Client
 * 
 * @author shoji
 *
 * 
 */
public class Ingest implements Closeable {

  private final Index index;
  private final WKTReader wktReader;
  private final int PRECISION = 1000000;

  public Ingest(String tableName, int splitThreshold) throws IOException {
    this.wktReader = new WKTReader();
    this.index = new Index(HBaseConfiguration.create(), tableName,
        splitThreshold);
  }


  /**
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {

    JSONObject jsonConf = JsonUtil.readLocalJSONFile(args[0]);
    String tableName = jsonConf.getString("tableName");
    String inputFile = jsonConf.getString("inputFile");// 导入数据
    int splitThreshold = jsonConf.getIntValue("splitThreshold");// 桶分割阈值
    boolean isUpdateIndex = jsonConf.getBoolean("isUpdateIndex");
    Ingest client = new Ingest(tableName, splitThreshold);

    if (!isUpdateIndex) {System.out.println("isUpdateIndex:"+false);}

    client.loadData(inputFile,isUpdateIndex);

    Closeables.closeQuietly(client);
  }

  public void loadData(String inputFile, boolean isUpdateIndex) throws Exception {

    System.out.println("write data.........");
    long records = 0;
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
    String line;
    Bucket bucket = new Bucket(index.getDataTable());
    while ((line = bufferedReader.readLine())!= null){
      String[] strings = line.split("@");
      long id = Long.parseLong(strings[0]);
      String geomWKT = strings[1];
      Coordinate coordinate = wktReader.read(geomWKT).getCoordinate();
      int integerLng = (int) ((coordinate.x+180.0) * PRECISION);
      int integerLat = (int) ((coordinate.y+90.0) * PRECISION);
      Point p = new Point(id, integerLng, integerLat);
      byte[] row = Utils.bitwiseZip(p.x, p.y);

      if (isUpdateIndex) {
        bucket = index.fetchBucket(row);
        bucket.insertMain(row, p);
        bucket.insertIndex(row, p);
      } else {
        bucket.insertMain(row, p);
      }
      if (++records % 100000 == 0){
        System.out.println("当前导入点数：" + records);
        System.out.println("当前桶分割次数：" + index.getSplitTimes());
      }
    }
    System.out.println("导入完成,总点数 ：" + records);
    System.out.println("桶分割总次数 ：" + index.getSplitTimes());
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
