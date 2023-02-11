import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;


public class GeoMesaQueryGeom {

    public static List<String> generateQueryBoxByRecords(String recordFile) {

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
                    String queryBox = "bbox (geom, "+ minLng + ", " + minLat +", " + maxLng + ", " + maxLat+ ")";
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


    public static void main(String[] args) throws Exception{
//        if (args.length < 3)
//            throw new IllegalArgumentException("请输入参数,1:records;2:catalog;3:typeName");

        String catalog = args[0];
        String typeName = args[1];
        List<String> queryBoxes = generateQueryBoxByRecords(args[2]);
        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog", catalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        System.out.println("**********query size:"+queryBoxes.size()+"**********");
        long totalTime = 0;
        for (String queryBox: queryBoxes) {
            long startTime  = System.currentTimeMillis();
            List<String> results = new ArrayList<>();
            Query query = new Query(typeName, ECQL.toFilter(queryBox));
            //获取读取器reader
            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
            while (reader.hasNext()){
                SimpleFeature feature = reader.next();
                String locationWKT = feature.getAttribute("geom").toString();
                results.add(locationWKT);
            }
            long endTime  = System.currentTimeMillis();
            long spendTime = endTime - startTime;
            totalTime+=spendTime;
            System.out.println("查询结果1：" + results.size()+"\t查询耗时：" + spendTime);
            System.out.println("------------------------");
        }
        long AVGTime = totalTime/queryBoxes.size();
        System.out.println("AVGTime: "+AVGTime);


//        for (String queryBox: queryBoxes){
//
//            Query query = new Query(typeName, ECQL.toFilter(queryBox));
//
//            long startTime  = System.currentTimeMillis();
//
//            //获取读取器reader
//            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
//
//            List<String> results = new ArrayList<>();
//
//            while (reader.hasNext()){
//                SimpleFeature feature = reader.next();
//                String locationWKT = feature.getAttribute("geom").toString();
//                results.add(locationWKT);
//            }
//            long endTime  = System.currentTimeMillis();
//
//            long spendTime = endTime - startTime;
//            totalTime+=spendTime;
//
//            System.out.println("查询结果数量：" + results.size());
//            System.out.println("查询耗时：" + spendTime);
//            System.out.println("------------------------");
//        }
//        long AVGTime = totalTime/queryBoxes.size();
//        System.out.println("AVGTime: "+AVGTime);
    }

    public static JSONObject readLocalJSONFile(String path) {
        File file = new File(path);
        StringBuilder sb = new StringBuilder();
        try {
            Reader reader = new InputStreamReader(new FileInputStream(file));
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return JSONObject.parseObject(sb.toString());
    }

}
