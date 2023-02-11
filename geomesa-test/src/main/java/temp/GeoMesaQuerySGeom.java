package temp;/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoMesaQuerySGeom implements Runnable {


    // 能跑通了，下一步复制一份，分存储、查询、查询一体；

    private final String catalog;
    private final String typeName;
    private final String recordFile;
    private final double minLng;
    private final double minLat;
    private final double maxLng;
    private final double maxLat;

    public GeoMesaQuerySGeom(String[] args) {
        // parse the data store parameters from the command line
        this.catalog = args[0];
        this.typeName = args[1];
        this.recordFile = args[2];
        this.minLng = Double.parseDouble(args[3]);
        this.minLat= Double.parseDouble(args[4]);
        this.maxLng= Double.parseDouble(args[5]);
        this.maxLat= Double.parseDouble(args[6]);
    }

    public List<String> generateQueryBoxByRecords() {

        List<String> queryBoxes = new ArrayList<>();
        try {
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
                    String queryBox = "bbox (geom, "+ minLng + " ," + minLat +", " + maxLng + ", " + maxLat+ ")";
                    queryBoxes.add(queryBox);
                }
            }

            // close the reader
            br.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return queryBoxes;
    }


    public List<String> generateQueryBoxByRecords1() {

        List<String> queryBoxes = new ArrayList<>();
        String queryBox = "bbox (geom, "+ minLng + " ," + minLat +", " + maxLng + ", " + maxLat+ ")";
        queryBoxes.add(queryBox);
        return queryBoxes;
    }


    @Override
    public void run() {
        DataStore datastore = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("hbase.catalog", catalog);
            datastore = createDataStore(params);
            List<String> queryBoxes = generateQueryBoxByRecords1();
//            List<String> queryBoxes = generateQueryBoxByRecords(recordFile);
            queryFeatures(datastore,queryBoxes);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        }
        System.out.println("Done");
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Loading datastore");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void queryFeatures(DataStore datastore,List<String> queryBoxes) throws IOException, CQLException {

        long totalTime = 0;
        for (String bbox:queryBoxes){

            long start = System.currentTimeMillis();

            Query query = new Query(typeName, ECQL.toFilter(bbox));

            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    datastore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            List<String> results = new ArrayList<>();

            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                String result = feature.getAttribute("geom").toString();
//                String result = DataUtilities.encodeFeature(feature);
                results.add(result);
            }
            long end = System.currentTimeMillis();

            long costTime = end-start;
            totalTime+=costTime;
            System.out.println("resultSize: "+results.size()+"\t costTime: "+costTime);
            System.out.println("-------------------------");
        }
        long AVGTime = totalTime/queryBoxes.size();
        System.out.println("AVGTime: "+AVGTime);
    }

    public static void main(String[] args) throws ParseException {
        new GeoMesaQuerySGeom(args).run();
    }
}
