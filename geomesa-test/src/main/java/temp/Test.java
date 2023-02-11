package temp;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

    public static void main(String[] args) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat(ResourceParseConsts.TIME_FORMAT);
        Date date = dateFormat.parse("2010-01-04 22:00:00");
        System.out.println(GeoMesaQuery.convertDate(date, true));
        System.out.println(GeoMesaQuery.convertDate(date, false));
    }

}
