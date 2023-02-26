/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Iterators;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.datastore.hbase.config.HBaseRequiredOptions;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.JsonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * This class is intended to provide a few examples on running Geowave queries of different types:
 * 1- Querying by polygon a set of points. 2- Filtering on attributes of features using CQL queries
 * 3- Ingesting polygons, and running polygon intersect queries. You can check all points,
 * geometries and query accuracy in a more visual manner @ http://geojson.io/
 */
public class Query {

  private static DataStore dataStore;

  public static void main(final String[] args) {
    JSONObject jsonConf = JsonUtil.readLocalJSONFile(args[0]);
    String nameSpace = jsonConf.getString("nameSpace");
    String inputFile = jsonConf.getString("inputFile");// 导入数据
    String geomType = jsonConf.getString("geomType");
    HBaseRequiredOptions options = new HBaseRequiredOptions();
    options.setZookeeper("huawei5:2181,huawei7:2181,huawei8:2181,huawei9:2181,huawei11:2181,huawei12:2181");
    options.setGeoWaveNamespace(nameSpace);
    dataStore = DataStoreFactory.createDataStore(options);
    List<Polygon> queryPolygons = generateQueryBoxByRecords(inputFile);
    rangeQuery(queryPolygons,geomType);
  }

  public static List<Polygon> generateQueryBoxByRecords(String recordFile) {

    List<Polygon> queryPolygons = new ArrayList<>();
    try {
      GeometryFactory geometryFactory = new GeometryFactory();
      // create a reader instance
      BufferedReader br = new BufferedReader(new FileReader(recordFile));
      // read until end of file
      String line;
      while ((line = br.readLine()) != null) {
        JSONObject jsonObj = JSONObject.parseObject(line);
        if (jsonObj.containsKey("minLng")) {
          double minLng = jsonObj.getDoubleValue("minLng");
          double minLat = jsonObj.getDoubleValue("minLat");
          double maxLng = jsonObj.getDoubleValue("maxLng");
          double maxLat = jsonObj.getDoubleValue("maxLat");
          Coordinate[] list  = new Coordinate[5];
          list[0] = new Coordinate(minLng,minLat);
          list[1] = new Coordinate(maxLng, minLat);
          list[2] = new Coordinate(maxLng, maxLat);
          list[3] = new Coordinate(minLng, maxLat);
          list[4] = new Coordinate(minLng, minLat);
          Polygon polygon = geometryFactory.createPolygon(list);
          queryPolygons.add(polygon);
        }
      }

      // close the reader
      br.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return queryPolygons;
  }


  /** This query will use a specific Bounding Box, and will find only 1 point. */
  private static void rangeQuery(List<Polygon> queryPolygons, String typeName) {

    long totalTime=0;
    for (Geometry queryPolygon: queryPolygons){
      long startTime = System.currentTimeMillis();
      final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
      try (CloseableIterator<SimpleFeature> iterator =
                   dataStore.query(bldr
                           .addTypeName(typeName)
                           .indexName("SPATIAL_IDX")
                           .addAuthorization("root")
                           .constraints(bldr.constraintsFactory()
                                   .spatialTemporalConstraints()
                                   .spatialConstraints(queryPolygon)
                                   .build())
                           .build())) {
        int cnt = 0;
        long endTime = System.currentTimeMillis();
        HashSet<String> featureSet = new HashSet<>();
        System.out.println("queryPolygon: "+queryPolygon.toText());
        while (iterator.hasNext()) {
          final SimpleFeature sf = iterator.next();
          featureSet.add(sf.getID());
          if (cnt<10){
            System.out.println(sf.getID()+": "+ sf.getDefaultGeometry());
          }
          cnt++;
        }
//        int count = Iterators.size(iterator);

        long costTime = endTime - startTime;
        totalTime+=costTime;
        System.out.println("cost: "+costTime+" count: "+featureSet.size());
        System.out.println("-------------------------\n");
      }
    }
    System.out.println("AVG_COST: "+totalTime/queryPolygons.size());
  }
}
