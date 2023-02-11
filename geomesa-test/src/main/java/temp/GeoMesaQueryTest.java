package temp;

import com.alibaba.fastjson.JSONObject;
import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeoMesaQueryTest {

    public static void main(String[] args) throws Exception{

        String minLng = args[0];
        String minLat = args[1];
        String maxLng = args[2];
        String maxLat = args[3];
        String catalog= args[4];
        String typeName= args[5];
        String queryBox = String.format("bbox(geom,%s,%s,%s,%s)", minLng,minLat,maxLng,maxLat);
        System.out.println("queryBox:\t"+queryBox);
        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog", catalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
//        Query query = new Query("example", Filter.INCLUDE);

        Query query = new Query(typeName, ECQL.toFilter(queryBox));

        //获取读取器reader
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

        List<String> results = new ArrayList<>();

        while (reader.hasNext()){
            SimpleFeature feature = reader.next();
            String locationWKT = feature.getAttribute("geom").toString();
            System.out.println("locationWKT:"+locationWKT);
            results.add(locationWKT);
        }

        System.out.println("查询结果数量：" + results.size());
    }

}
