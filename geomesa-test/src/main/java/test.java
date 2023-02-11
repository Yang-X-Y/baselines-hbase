import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

public class test {

    public static void main(String[] args) {
        SimpleFeatureType sft = GeoMesaKNNQuery.getSimpleFeatureType("test", "Point");
        DefaultFeatureCollection inputFeatures = GeoMesaKNNQuery.getSimpleFeatureCollection(sft,"Point (12.1 44.5)");
        SimpleFeatureIterator features = inputFeatures.features();
        while (features.hasNext()){
            System.out.println(features.next().toString());
        }
    }
}
