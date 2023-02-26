import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.datastore.hbase.config.HBaseRequiredOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import java.util.ArrayList;
import java.util.List;

public class Ingest {

    public DataStore connectDatabase(){
        HBaseRequiredOptions options = new HBaseRequiredOptions();
        options.setZookeeper("huawei5:2181,huawei7:2181,huawei8:2181,huawei9:2181,huawei11:2181,huawei12:2181");
        options.setGeoWaveNamespace("geowave");
        DataStore myStore = DataStoreFactory.createDataStore(options);
//        final DataStore geowaveDataStore =
//                DataStoreFactory.createDataStore(new MemoryRequiredOptions());
        return myStore;
    }

    public Index setIndex() throws FactoryException {
        // Spatial Index
        SpatialIndexBuilder spatialIndexBuilder = new SpatialIndexBuilder();
        spatialIndexBuilder.setCrs("EPSG:4326");
        Index spatialIndex = spatialIndexBuilder.createIndex();
//        Index spatialIndex = new SpatialIndexBuilder().createIndex();
        return spatialIndex;
    }

    public List<SimpleFeature> buildSimpleFeature(){

        List<SimpleFeature> features = new ArrayList<>();
        SimpleFeatureTypeBuilder pointTypeBuilder = new SimpleFeatureTypeBuilder();
        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
        pointTypeBuilder.setName("TestPointType");
        pointTypeBuilder.add(attributeBuilder.binding(Point.class).nillable(false).buildDescriptor("the_geom"));
        SimpleFeatureType pointType = pointTypeBuilder.buildFeatureType();
        // Create a feature builder
        SimpleFeatureBuilder pointFeatureBuilder = new SimpleFeatureBuilder(pointType);

        GeometryFactory factory = new GeometryFactory();
        pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(1, 1)));
        SimpleFeature feature1 = pointFeatureBuilder.buildFeature("feature1");
        features.add(feature1);
        System.out.println("feature1:"+feature1.toString());

        pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(5, 5)));
        SimpleFeature feature2 = pointFeatureBuilder.buildFeature("feature2");
        features.add(feature2);
        System.out.println("feature2:"+feature2.toString());

        pointFeatureBuilder.set("the_geom", factory.createPoint(new Coordinate(-5, -5)));
        SimpleFeature feature3 = pointFeatureBuilder.buildFeature("feature3");
        features.add(feature3);
        System.out.println("feature3:"+feature3.toString());

        return features;

    }

    public void write(DataStore myStore,Index spatialIndex,List<SimpleFeature> features){

        SimpleFeatureType pointType = features.get(0).getFeatureType();

        // Create an adapter for point type
        FeatureDataAdapter pointTypeAdapter = new FeatureDataAdapter(pointType);

        // Add the point type to the data store in the spatial index
        myStore.addType(pointTypeAdapter, spatialIndex);
        System.out.println("success:myStore.addType");

        Writer<SimpleFeature> writer = myStore.createWriter(pointTypeAdapter.getTypeName());
        System.out.println("success:createWriter");
        for (SimpleFeature feature: features){

            writer.write(feature);
        }
        writer.close();
    }

    public static void main(String[] args) throws FactoryException {
        // Create a point feature type
        Ingest ingest = new Ingest();

        DataStore myStore = ingest.connectDatabase();
        System.out.println("success:connectDatabase");
        Index spatialIndex = ingest.setIndex();
        System.out.println("success:setIndex");
        myStore.addIndex(spatialIndex);
        System.out.println("success:addIndex");
        List<SimpleFeature> features = ingest.buildSimpleFeature();
        System.out.println("success:buildSimpleFeature");
        ingest.write(myStore,spatialIndex,features);
        System.out.println("success:write");
    }
}
