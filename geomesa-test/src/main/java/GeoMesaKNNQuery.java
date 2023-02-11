import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory;
import org.locationtech.geomesa.process.knn.KNNQuery;
import org.locationtech.geomesa.process.knn.KNearestNeighborSearchProcess;
import org.locationtech.geomesa.process.knn.NearestNeighbors;
import org.locationtech.geomesa.process.knn.SimpleFeatureWithDistance;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import scala.Tuple2;
import util.JsonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeoMesaKNNQuery {

    public static SimpleFeatureType getSimpleFeatureType(String typeName,String geom) {
        SimpleFeatureType sft = SimpleFeatureTypes.createType(typeName,
                String.format("dtg:Date,*geom:%s:srid=4326", geom));
        return sft;
    }

    public static DefaultFeatureCollection getSimpleFeatureCollection(SimpleFeatureType sft, String queryPoint) {
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection(sft.getTypeName(), sft);
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        builder.set("geom", queryPoint);
        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        SimpleFeature feature = builder.buildFeature("queryPoint");
        featureCollection.add(feature);
        return featureCollection;
    }

    public static SimpleFeature createSimpleFeature(SimpleFeatureType sft, String queryPoint) {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        builder.set("geom", queryPoint);
        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        SimpleFeature feature = builder.buildFeature("queryPoint");
        return feature;
    }
    
    public static List<Tuple2<Double, Double>> generateQueryPointsByRecords(String recordFile) {

        List<Tuple2<Double, Double>> queryPoints = new ArrayList<>();
        try {
            // create a reader instance
            BufferedReader br = new BufferedReader(new FileReader(recordFile));
            // read until end of file
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject jsonObj = JSONObject.parseObject(line);
                if (jsonObj.containsKey("pointLng")) {
                    double pointLng = jsonObj.getDoubleValue("pointLng");
                    double pointLat = jsonObj.getDoubleValue("pointLat");
                    queryPoints.add(new Tuple2<>(pointLng,pointLat));
                }
            }

            // close the reader
            br.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return queryPoints;
    }


    public static void main(String[] args) throws Exception {
        JSONObject jsonConf = JsonUtil.readLocalJSONFile(args[0]);
        String recordFile = jsonConf.getString("recordFile");
        int numK = jsonConf.getIntValue("numK");
        double looseQueryRange = jsonConf.getDoubleValue("looseQueryRange");
        double estimatedDistance = jsonConf.getDoubleValue("estimatedDistance");
        double maxSearchDistance = jsonConf.getDoubleValue("maxSearchDistance");
        String catalog = jsonConf.getString("catalog");
        String typeName = jsonConf.getString("typeName");
        List<Tuple2<Double, Double>> queryPoints = generateQueryPointsByRecords(recordFile);
        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog", catalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        System.out.println("**********query size:" + queryPoints.size() + "**********");
        long totalTime = 0;
        SimpleFeatureType sft = getSimpleFeatureType(typeName, "Point");

        for (Tuple2<Double, Double> lonLatTuple : queryPoints) {

//            List<String> results = new ArrayList<>();
//            KNearestNeighborSearchProcess knnProcess = new KNearestNeighborSearchProcess();
            String queryPoint = "Point (" + lonLatTuple._1 + " " + lonLatTuple._2 + ")";
            String queryBox = "bbox (geom, " + (lonLatTuple._1 - looseQueryRange) + " ," + (lonLatTuple._2 - looseQueryRange) + ", " + (lonLatTuple._1 + looseQueryRange) + ", " + (lonLatTuple._2 + looseQueryRange) + ")";
            Query looseQuery = new Query(typeName, ECQL.toFilter(queryBox));
            long startTime = System.currentTimeMillis();
            NearestNeighbors knnResults = KNNQuery.runNewKNNQuery(dataStore.getFeatureSource(sft.getTypeName()),
                    looseQuery, numK, estimatedDistance, maxSearchDistance,
                    createSimpleFeature(sft, queryPoint));
            scala.collection.immutable.List<SimpleFeatureWithDistance> results = knnResults.getK().toList();
            long endTime = System.currentTimeMillis();
            long spendTime = endTime - startTime;
            StringBuilder queryResultStr = new StringBuilder("");
            queryResultStr.append("resultSize: ").append(results.size());
            for (int i=0;i<results.size();i++){
                SimpleFeatureWithDistance result = results.apply(i);
                queryResultStr.append("; pointID: ");
                queryResultStr.append(result.sf().getID());
                queryResultStr.append("; distance: ");
                queryResultStr.append(result.dist());
                queryResultStr.append("; geom: ");
                queryResultStr.append(result.sf().getDefaultGeometry().toString());
                queryResultStr.append(" || ");
            }


//            SimpleFeatureCollection inputFeatures = getSimpleFeatureCollection(sft, queryPoint);
//            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(looseQuery, Transaction.AUTO_COMMIT);
//            List<SimpleFeature> looseQueryFeatures = new ArrayList<>();
//            while (reader.hasNext()){
//                looseQueryFeatures.add(reader.next());
//            }
//            SimpleFeatureCollection dataFeatures = new ListFeatureCollection(sft, looseQueryFeatures);
//
//            System.out.println("looseQueryFeatures:"+looseQueryFeatures.size());
//
////            SimpleFeatureCollection dataFeatures = dataStore.getFeatureSource(sft.getTypeName()).getFeatures();
//            System.out.println("inputFeatures:"+inputFeatures.size());
//            System.out.println("dataFeatures:"+dataFeatures.size());
//            SimpleFeatureIterator knnResult = knnProcess.execute(inputFeatures, dataFeatures, numK, 0.0, maxSearchDistance).features();
//            while (knnResult.hasNext()) {
//                SimpleFeature feature = knnResult.next();
//                String locationWKT = feature.getAttribute("geom").toString();
//                results.add(locationWKT);
//            }

            totalTime += spendTime;
            System.out.println("查询耗时：" + spendTime + "\t查询结果：" + queryResultStr.toString());
            System.out.println("------------------------");
        }
        long AVGTime = totalTime / queryPoints.size();
        System.out.println("AVGTime: " + AVGTime);
    }

}
