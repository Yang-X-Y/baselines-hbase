/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */

import com.alibaba.fastjson.JSONObject;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import util.JsonUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * This class is intended to provide a few examples on running Geowave queries of different types:
 * 1- Querying by polygon a set of points. 2- Filtering on attributes of features using CQL queries
 * 3- Ingesting polygons, and running polygon intersect queries. You can check all points,
 * geometries and query accuracy in a more visual manner @ http://geojson.io/
 */
public class Ingest {

  private static DataStore dataStore;

  public static void main(final String[] args) throws ParseException, CQLException, IOException {
    JSONObject jsonConf = JsonUtil.readLocalJSONFile(args[0]);
    String nameSpace = jsonConf.getString("nameSpace");
    String inputFile = jsonConf.getString("inputFile");// 导入数据
    String geomType = jsonConf.getString("geomType");// Point/Polygon/LineString

    HBaseRequiredOptions options = new HBaseRequiredOptions();
    options.setZookeeper("huawei5:2181,huawei7:2181,huawei8:2181,huawei9:2181,huawei11:2181,huawei12:2181");
//    options.setZookeeper("huawei5:2181");
    options.setGeoWaveNamespace(nameSpace);
    dataStore = DataStoreFactory.createDataStore(options);
    ingestGeometry(inputFile,geomType);
  }

  private static void ingestGeometry(String inputFile, String geomType) {
    // First, we'll build our first kind of SimpleFeature, which we'll call
    // "basic-feature"
    // We need the type builder to build the feature type
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    // AttributeTypeBuilder for the attributes of the SimpleFeature
    final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
    // Here we're setting the SimpleFeature name. Later on, we'll be able to
    // query GW just by this particular feature.
    sftBuilder.setName(geomType);
    // Add the attributes to the feature
    // Add the geometry attribute, which is mandatory for GeoWave to be able
    // to construct an index out of the SimpleFeature
    sftBuilder.add(attrBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));


    // Create the SimpleFeatureType
    final SimpleFeatureType sfType = sftBuilder.buildFeatureType();
    // We need the adapter for all our operations with GeoWave
    final FeatureDataAdapter sfAdapter = new FeatureDataAdapter(sfType);

    // Now we build features.

    Index index = new SpatialIndexBuilder().createIndex();
    dataStore.addType(sfAdapter, index);

    int records = 0;
    try {
      WKTReader wktReader = new WKTReader();
      Writer<SimpleFeature> indexWriter = dataStore.createWriter(sfAdapter.getTypeName());
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
      String line;
      while ((line = bufferedReader.readLine())!= null){
        String[] strings = line.split("@");
        String id = strings[0];
        String geomWKT = strings[1];
        final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(sfType);
        sfBuilder.set("geometry", wktReader.read(geomWKT));
        // When calling buildFeature, we need to pass an unique id for that
        // feature, or it will be overwritten.
        SimpleFeature tempSF = sfBuilder.buildFeature(id);
        indexWriter.write(tempSF);

        if (++records % 1000000 == 0){
          System.out.println("目前导入点数：" + records);
        }
      }
      indexWriter.close();
      System.out.println("导入总点数：" + records);
    }catch (IOException | ParseException e){
      System.out.println(e.getMessage());
    }

  }
}
