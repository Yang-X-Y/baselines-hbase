package tiny.mdhbase.bulkload;

import com.google.common.collect.Iterators;
import javafx.util.Pair;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;
import java.util.Iterator;

public class readGeometry implements Serializable, FlatMapFunction<Iterator<String>, Pair<String,Geometry>> {

    @Override
    public Iterator<Pair<String,Geometry>> call(Iterator<String> iterator) throws Exception {
        WKTReader wktReader = new WKTReader();
        return Iterators.transform(iterator, s -> {
            try {
                String[] stringSplit = s.split("@");
                String objId = stringSplit[0];
                Geometry geometry = wktReader.read(stringSplit[1]);
                return new Pair<>(objId, geometry);
            }catch (ParseException e){
                throw new RuntimeException(e);
            }
        });
    }

}

