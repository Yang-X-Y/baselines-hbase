package temp;/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class GeoMesaQuickStart implements Runnable {


    // 能跑通了，下一步复制一份，分存储、查询、查询一体；

    private final String catalog;
    private final String typeName;
    private final String inputFile;
    private final String geomType;
    private final String precision;
    private final String bbox;
    private final boolean cleanup;

    public GeoMesaQuickStart(String[] args) {
        // parse the data store parameters from the command line
        this.inputFile = args[0];
        this.catalog = args[1];
        this.typeName = args[2];
        this.geomType = args[3];
        this.precision = args[4];
        this.bbox = args[5];
        this.cleanup = Boolean.parseBoolean(args[6]);
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("hbase.catalog", catalog);
            datastore = createDataStore(params);
            SimpleFeatureType sft = getSimpleFeatureType();
            createSchema(datastore, sft);
            List<SimpleFeature> features = getTestFeatures();
            writeFeatures(datastore, sft, features);
            List<Query> queries = getTestQueries();
            queryFeatures(datastore,queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, typeName, cleanup);
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

    public SimpleFeatureType getSimpleFeatureType() {
        StringBuilder attributes = new StringBuilder();
        attributes.append("geomId:String,");
//        attributes.append("dtg:Date,");
        attributes.append(String.format("*geom:%s:srid=4326", geomType)); // the "*" denotes the default geometry (used for indexing)

        // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
        // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
        SimpleFeatureType sft = SimpleFeatureTypes.createType(typeName, attributes.toString());
//        sft.getUserData().put("geomesa.xz.precision", precision);
//        sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        return sft;
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public List<SimpleFeature> getTestFeatures() {

        System.out.println("Generating test data");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String commonDate="2022-10-01 12:00:00";
        List<SimpleFeature> features = new ArrayList<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

            String line;
            while ((line = bufferedReader.readLine())!= null){
                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());
                String[] strings = line.split("@");
                String geomId = strings[0];
                String geomWKT = strings[1];
                // pull out the fields corresponding to our simple feature attributes
                builder.set("geomId", geomId);
                // we can use WKT (well-known-text) to represent geometries
                // note that we use longitude first ordering
                builder.set("geom", geomWKT);

                // 设置时间
//                builder.set("dtg", sdf.parse(commonDate));
                // be sure to tell GeoTools explicitly that we want to use the ID we provided
                builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
                // build the feature - this also resets the feature builder for the next entry
                // use the GLOBALEVENTID as the feature ID
                SimpleFeature feature = builder.buildFeature(geomId);
                features.add(feature);
            }
        }catch (IOException e){
            System.out.println(e.getMessage());
        }
        System.out.println();
        return features;
    }

    public List<Query> getTestQueries() throws CQLException {

        List<Query> queries = new ArrayList<>();

        String queryBox = "bbox(geom,"+bbox+")";
        queries.add(new Query(typeName, ECQL.toFilter(queryBox)));
        return queries;
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            System.out.println("Writing test data");
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                     ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                     toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
            System.out.println("Wrote " + features.size() + " features");
            System.out.println();
        }
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException, CQLException {

        for (Query query: queries){
            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    datastore.getFeatureReader(query, Transaction.AUTO_COMMIT);
            // loop through all results, only print out the first 10
            int n = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                if (n++ < 10) {
                    // use geotools data utilities to get a printable string
                    System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                } else if (n == 10) {
                    System.out.println("...");
                }
            }
            System.out.println();
            System.out.println("Returned " + n + " total features");
            System.out.println();
        }

    }

    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    System.out.println("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        new GeoMesaQuickStart(args).run();
    }
}
