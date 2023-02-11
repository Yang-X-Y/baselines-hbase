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


public class GeoMesaQuery {

    public static void main(String[] args) throws Exception{
        if (args.length < 1)
            throw new IllegalArgumentException("请输入查询文件");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        JSONObject jsonObject = readLocalJSONFile(args[0]);
//        jsonObject = jsonObject.getJSONObject(temp.ResourceParseConsts.QUERY_CONDITION);

        double minLng = jsonObject.getDoubleValue(ResourceParseConsts.MIN_LONGITUDE);
        double minLat = jsonObject.getDoubleValue(ResourceParseConsts.MIN_LATITUDE);
        double maxLng = jsonObject.getDoubleValue(ResourceParseConsts.MAX_LONGITUDE);
        double maxLat = jsonObject.getDoubleValue(ResourceParseConsts.MAX_LATITUDE);

        Date startDateTime = null;
        Date endDateTime = null;

        startDateTime = sdf.parse(jsonObject.getString(ResourceParseConsts.START_TIME));
        endDateTime = sdf.parse(jsonObject.getString(ResourceParseConsts.END_TIME));

        //日期转换成UTC格式进行查询
        String timeRange = convertDate(startDateTime, true) + "/" + convertDate(endDateTime, false);

        String during = "dtg DURING " + timeRange;
        String bbox = "bbox (geom, "+ minLng + " ," + minLat +", " + maxLng + ", " + maxLat+ ")";
        String spatioTemp = bbox + " AND " + during;

        System.out.println(spatioTemp);

        Query query = new Query("YellowTripData2010", ECQL.toFilter(spatioTemp));

        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog", "geomesa1");
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);

        long t  = System.currentTimeMillis();

        //获取读取器reader
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

        List<String> results = new ArrayList<>();

        while (reader.hasNext()){
            SimpleFeature feature = reader.next();
            String locationWKT = feature.getAttribute("geom").toString();
            Date date = (Date)feature.getAttribute("dtg");
            results.add(sdf.format(date) + "  " + locationWKT);
        }
        System.out.println("查询结果数量：" + results.size());
        System.out.println("查询耗时：" + (System.currentTimeMillis() - t));
        int id = 1;
        boolean flag = true;
        for (String res : results) {
            if (id > 10 && flag) {
                System.out.println("and " + (results.size() - 10) + " more objects...");
                System.out.println("Show them all? Press yes(y) or no(n) >> ");
                Scanner scanner = new Scanner(System.in);
                String yesOrNo = scanner.next();
                if (yesOrNo.equals("yes") || yesOrNo.equals("y")) {
                    flag = false;
                } else {
                    break;
                }
            }
            System.out.println(id + " " + res);
            id++;
        }


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
