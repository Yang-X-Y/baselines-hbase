package temp;/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.*;

public class GeoMesaQueryQuickStart implements Runnable {

    private final String catalog;
    private final String typeName;

    public GeoMesaQueryQuickStart(String[] args) {
        // parse the data store parameters from the command line
        this.catalog = args[0];
        this.typeName = args[1];
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("hbase.catalog", catalog);
            datastore = createDataStore(params);
            queryFeatures(datastore);
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

    public void queryFeatures(DataStore datastore) throws IOException, CQLException {

        String bbox = "bbox(geom,-180,-90,180,90)";
        // basic spatio-temporal query
        Query query = new Query(typeName, ECQL.toFilter(bbox));
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

    public static void main(String[] args) throws ParseException {
        new GeoMesaQueryQuickStart(args).run();
    }
}
