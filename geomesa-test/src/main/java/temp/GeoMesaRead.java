package temp;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import scala.reflect.internal.Trees;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GeoMesaRead {
    public static void main(String[] args) throws Exception{

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");    //格式化规则

        //构建GeoMesa连接--DataStroe
        Map<String, String> parameters = new HashMap<>();
        parameters.put("hbase.catalog", "geomesa");
        DataStore dataStore = DataStoreFinder.getDataStore(parameters);
        SimpleFeatureSource simpleFeatureSource = dataStore.getFeatureSource("YellowTripData2010");
        SimpleFeatureIterator iterator = simpleFeatureSource.getFeatures().features();
        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            feature.getProperties().forEach( a ->{
                        if (a.getName().toString().equals("dtg")){
                            Date date = (Date)a.getValue();
                            System.out.print(a.getName().toString() + " " + sdf.format(date)+ "  |   ");
                        }else
                            System.out.print(a.getName().toString() + " " + a.getValue().toString() + "  |   ");

                    }
            );
            System.out.println("");
        }
    }
}
