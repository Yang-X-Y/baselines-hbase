package temp;

import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;


public class GeoMesaQuerySTGeom {

    public static void main(String[] args) throws Exception{

        String catalog = args[0];
        String typeName = args[1];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startDateTime = sdf.parse("2010-01-27 00:00:00");
        Date endDateTime = sdf.parse("2010-01-27 23:59:59");
        //日期转换成UTC格式进行查询
        String timeRange = convertDate(startDateTime, true) + "/" + convertDate(endDateTime, false);
        String during = "dtg DURING " + timeRange;
        String bbox = "bbox(geom,-74,40,-73,41)";
//        String spatioTemp = bbox + " AND " + during;
        String spatioTemp = bbox;


        System.out.println(spatioTemp);

        Query query = new Query(typeName, ECQL.toFilter(spatioTemp));

        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog",catalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);

        long t  = System.currentTimeMillis();

        //获取读取器reader
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

        List<String> results = new ArrayList<>();

        while (reader.hasNext()){
            SimpleFeature feature = reader.next();
            String locationWKT = feature.getAttribute("geom").toString();
            Date date = (Date)feature.getAttribute("dtg");
            String result = sdf.format(date) + "  " + locationWKT;
            System.out.println("result:"+result);
            results.add(result);
        }
        System.out.println("查询结果数量：" + results.size());
        System.out.println("查询耗时：" + (System.currentTimeMillis() - t));
//        int id = 1;
//        boolean flag = true;
//        for (String res : results) {
//            if (id > 10 && flag) {
//                System.out.println("and " + (results.size() - 10) + " more objects...");
//                System.out.println("Show them all? Press yes(y) or no(n) >> ");
//                Scanner scanner = new Scanner(System.in);
//                String yesOrNo = scanner.next();
//                if (yesOrNo.equals("yes") || yesOrNo.equals("y")) {
//                    flag = false;
//                } else {
//                    break;
//                }
//            }
//            System.out.println(id + " " + res);
//            id++;
//        }


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

    public static String convertDate(Date date, boolean minus){
        Instant instant = date.toInstant();
        String s = instant.toString();
        return s;
    }


}
