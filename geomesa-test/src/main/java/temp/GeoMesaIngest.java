package temp;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.util.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;



public class GeoMesaIngest {
    /**
     * 这个方法主要设定了表名"index-text02"，
     * 和schema结构"taxiId:String,dtg:Date,*geom:Point:srid=4326"
     * @return SimpleFeatureType，即建表的schema表结构
     */
    public SimpleFeatureType getSimpleFeatureType() {
        SimpleFeatureType sft = SimpleFeatureTypes.createType("YellowTripData2010",
                "dtg:Date,*geom:Point:srid=4326");
        //sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        //设置xz3索引时间间隔为天
//        sft.getUserData().put("geomesa.xz3.interval", "day");
        //设置时空索引时间字段为date
        //sft.getUserData().put("geomesa.index.dtg", "dateAttr");
        //设置索引精度
        //sft.getUserData().put("geomesa.xz.precision", 10);
        return sft;
    }

    public static void main(String[] args) {
        if (args.length < 1)
            throw new IllegalArgumentException("请输入数据文件");

        String[] inputFiles = args[0].split(",");

        GeoMesaIngest demo01 = new GeoMesaIngest();

        try {
            //创建datastore
            Map<String, Serializable> parameters = new HashMap<>();
            parameters.put("hbase.catalog", "geomesa1");
            DataStore datastore = DataStoreFinder.getDataStore(parameters);

            // 创建schema
            SimpleFeatureType sft = demo01.getSimpleFeatureType();
            System.out.println(sft);
            System.out.println(datastore);
            datastore.createSchema(sft);

            //获取Features  
            //List<SimpleFeature> features = demo01.getData(inputFiles);

            //写入Features  
            demo01.writeFeature(datastore, sft, inputFiles);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 这个方法主要用来将封装好的feature存入到对应的datastore中
     * @param datastore 数据源，可以选择的有很多，此处是HBaseDataStore
     * @param sft 表结构
     * @param
     */
    private void writeFeature(DataStore datastore, SimpleFeatureType sft, String[] inputFiles) throws Exception {


        System.out.println("write data");
        long points = 0;
        FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for(String inputFile : inputFiles){
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
                String line;
                while ((line = bufferedReader.readLine())!= null){
                    String[] strings = line.split(",");
                    String date = strings[0];
                    double lng = Double.parseDouble(strings[1]);
                    double lat = Double.parseDouble(strings[2]);
                    if (lng < -180 || lng > 180 || lat < -90 || lat > 90) continue;

                    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());
                    try {
                        builder.set("dtg", sdf.parse(date));
                    }catch (DateTimeParseException e){
                        System.out.println(e.getMessage());
                        continue;
                    }

                    builder.set("geom", "POINT (" + lng + " " + lat + ")");
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                    SimpleFeature feature = builder.buildFeature(UUID.randomUUID().toString());

                    SimpleFeature toWrite = writer.next();
                    toWrite.setAttributes(feature.getAttributes());
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                    toWrite.getUserData().putAll(feature.getUserData());
                    writer.write();
                    if (++points % 10000 == 0){
                        System.out.println("导入点数 ：" + points);
                    }
                }
            }catch (IOException e){
                System.out.println(e.getMessage());
            }
        }
        // 关闭流
        writer.close();

        System.out.println("导入完成，点数 ：" + points);


    }

    /**
     * 这个方法主要是将非结构化的数据转换为feature对象
     * @return feature对象
     */
  /*  private List<SimpleFeature> getData(String[] inputFiles) throws IOException{

        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for(String inputFile : inputFiles){
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
            bufferedReader.lines().forEach((line) -> {
                String[] strings = line.split(",");
                String date = strings[1];
                String lng = strings[5];
                String lat = strings[6];
                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());
                builder.set("dtg", Date.from(LocalDateTime.parse(date, dateFormat).toInstant(ZoneOffset.UTC)));
                builder.set("geom", "POINT (" + lng + " " + lat + ")");
                builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                SimpleFeature feature = builder.buildFeature(UUID.randomUUID().toString());

            });
        }

    }*/

}