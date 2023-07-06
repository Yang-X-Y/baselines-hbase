import com.alibaba.fastjson.JSONObject;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.process.knn.KNNQuery;
import org.locationtech.geomesa.process.knn.NearestNeighbors;
import org.locationtech.geomesa.process.knn.SimpleFeatureWithDistance;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import scala.Tuple2;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeoMesaKNNQueryNew {

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

    /**
     *
     * @param lonLatTuple
     * @param bearingInDegrees
     * @param distanceInMeters
     * @return
     */
    public static Tuple2<Double, Double> moveInDirection(Tuple2<Double, Double> lonLatTuple, double bearingInDegrees, double distanceInMeters) {

        double degToRad = 0.0174532925199433;

        if (bearingInDegrees < 0 || bearingInDegrees > 360) {
            throw new IllegalArgumentException("direction must be in (0,360)");
        }

        double a = 6378137, b = 6356752.3142, f = 1 / 298.257223563; // WGS-84
        // ellipsiod
        double alpha1 = bearingInDegrees * degToRad;
        double sinAlpha1 = Math.sin(alpha1), cosAlpha1 = Math.cos(alpha1);

        double tanU1 = (1 - f) * Math.tan(lonLatTuple._2 * degToRad);
        double cosU1 = 1 / Math.sqrt((1 + tanU1 * tanU1)), sinU1 = tanU1 * cosU1;
        double sigma1 = Math.atan2(tanU1, cosAlpha1);
        double sinAlpha = cosU1 * sinAlpha1;
        double cosSqAlpha = 1 - sinAlpha * sinAlpha;
        double uSq = cosSqAlpha * (a * a - b * b) / (b * b);
        double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
        double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));

        double sinSigma = 0, cosSigma = 0, cos2SigmaM = 0;
        double sigma = distanceInMeters / (b * A), sigmaP = 2 * Math.PI;
        while (Math.abs(sigma - sigmaP) > 1e-12) {
            cos2SigmaM = Math.cos(2 * sigma1 + sigma);
            sinSigma = Math.sin(sigma);
            cosSigma = Math.cos(sigma);
            double deltaSigma = B
                    * sinSigma
                    * (cos2SigmaM + B
                    / 4
                    * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - B / 6 * cos2SigmaM
                    * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));
            sigmaP = sigma;
            sigma = distanceInMeters / (b * A) + deltaSigma;
        }

        double tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1;
        double lat2 = Math.atan2(sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1, (1 - f)
                * Math.sqrt(sinAlpha * sinAlpha + tmp * tmp));
        double lambda = Math.atan2(sinSigma * sinAlpha1, cosU1 * cosSigma - sinU1 * sinSigma * cosAlpha1);
        double C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
        double L = lambda - (1 - C) * f * sinAlpha
                * (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));

        double newLat = lat2 / degToRad;
        double newLon = lonLatTuple._1 + L / degToRad;

        newLon = (newLon > 180.0 ? newLon - 360 : newLon);
        newLon = (newLon < -180.0 ? 360.0 + newLon : newLon);

        return new Tuple2<>(newLon, newLat);
    }


    public static void main(String[] args) throws Exception {
        JSONObject jsonConf = JSONUtil.readLocalJSONFile(args[0]);
        String recordFile = jsonConf.getString("recordFile");
        int numK = jsonConf.getIntValue("numK");
//        double looseQueryRange = jsonConf.getDoubleValue("looseQueryRange");
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
            // 根据maxSearchDistance 计算 looseQueryRange
            Tuple2<Double, Double> upperRight = moveInDirection(lonLatTuple, 45,
                    Math.sqrt(2) * maxSearchDistance);
            Tuple2<Double, Double> bottomLeft = moveInDirection(lonLatTuple, 225,
                    Math.sqrt(2) * maxSearchDistance);
            String queryPoint = "Point (" + lonLatTuple._1 + " " + lonLatTuple._2 + ")";
            String queryBox = "bbox (geom, " + bottomLeft._1 + " ," + bottomLeft._2 + ", " + upperRight._1 + ", " + upperRight._2 + ")";
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
