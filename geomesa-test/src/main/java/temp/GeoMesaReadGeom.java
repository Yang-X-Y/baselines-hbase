package temp;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GeoMesaReadGeom {
    public static void main(String[] args) throws Exception{

        //构建GeoMesa连接--DataStroe
        String catalog = args[0];
        String typeName = args[1];
        int count = Integer.parseInt(args[2]);
        Map<String, String> parameters = new HashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");    //格式化规则
        parameters.put("hbase.catalog", catalog);
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        SimpleFeatureSource simpleFeatureSource = dataStore.getFeatureSource(typeName);
        SimpleFeatureIterator iterator = simpleFeatureSource.getFeatures().features();
        int t=0;
        while (iterator.hasNext() && t<count) {
            SimpleFeature feature = iterator.next();
            feature.getProperties().forEach( a ->{
                if (a.getName().toString().equals("dtg")){
                    Date date = (Date)a.getValue();
                    System.out.print(a.getName().toString() + " " + sdf.format(date)+ "  |   ");
                }else
                    System.out.print(a.getName().toString() + " " + a.getValue().toString() + "  |   ");

            });
            System.out.print(" ");
            t++;
        }
    }
}
