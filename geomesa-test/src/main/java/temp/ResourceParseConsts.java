package temp;

/**
 * @author Shen dannan
 * Create on 1/17/21.
 */
public class ResourceParseConsts {
    // constants related to table structure
    public static final String META_COMMON_FAMILY = "A";
    public static final String META_OPTIONAL_COLUMN_FAMILY = "B";
    public static final String META_UPDATE_FAMILY = "U";
    public static final String META_QUERY_FAMILY = "Q";
    public static final String COLUMN_FAMILY = "F";
    public static final String INDEX_QUALIFIER = "O";
    public static final String PREFIX_LENGTH = "PL";
    public static final String POINT_NUM = "PN";
    public static final String MINIMUM_BOUNDING_RECTANGLE = "MBR";
    public static final String TIME_GRANULARITY = "TG";
    public static final String DATA_TYPE_T = "DT";
    public static final String GAMA_T = "GM";
    public static final String SPACE_THRESHOLD_T = "T1";
    public static final String TIME_THRESHOLD_T = "T2";
    public static final String TOTAL_COUNT_T = "TC";
    public static final String DESCRIPTION_T = "DD";
    public static final String BOUNDING_BOX_T = "BB";
    public static final String TIME_RANGE_T = "TR";
    public static final String DATA_NODE_T = "DN";
    public static final String QUAD_TREE = "QT";

    // constants related to table name
    public static final String META_TABLE_KEY = "MetaData";
    public static final String SECONDARY_INDEX_SUFFIX = "_secondary_index_";
    public static final String INDEX_TABLE_SUFFIX = "_index_";

    // constants related to parse the json file of creating dataset
    public static final String DATA_SET_NAME = "name";
    public static final String DATA_TYPE = "data_type";
    public static final String BOUNDING_BOX = "bounding_box";
    public static final String TOTAL_COUNT = "count";
    public static final String TIME_RANGE = "time_range";
    public static final String TIME_SCHEMA = "time_schema";
    public static final String DESCRIPTION = "description";
    public static final String ATTRIBUTES = "attributes";
    public static final String GAMA = "gama";
    public static final String MATRIX_PA = "PA";
    public static final String MATRIX_Aold = "A";
    public static final String Z2_QUAD_TREE = "QT";
    public static final String TIME_THRESHOLD = "time_threshold";
    public static final String SPACE_THRESHOLD = "space_threshold";
    public static final String DATA_NODE = "data_node";

    // constants related to parse the json file of querying
    public static final String QUERY_TYPE = "query_type";
    public static final String QUERY_CONDITION = "query_condition";
    public static final String ST_QUERY = "st_query";
    public static final String S_QUERY = "spatial_query";
    public static final String KNN_QUERY = "knn_query";
    public static final String MIN_LATITUDE = "min_lat";
    public static final String MIN_LONGITUDE = "min_lng";
    public static final String MAX_LATITUDE = "max_lat";
    public static final String MAX_LONGITUDE = "max_lng";
    public static final String MAX_RECURSIVE = "maxRecursive";
    public static final String START_TIME = "start_time";
    public static final String END_TIME = "end_time";
    public static final String LONGITUDE = "lng";
    public static final String LATITUDE = "lat";
    public static final String K = "k";
    public static final String RADIUS = "radius";
    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String USE_ADAPTIVE_INDEX = "useAdaptiveIndex";

    // constants related to parse the json file of ingesting
    public static final String INPUT_PATH = "input_path";
    public static final String OUTPUT_PATH = "output_path";
    public static final String THRESHOLD = "threshold";
    public static final String START_BIT_PRECISION = "start_bit_precision";
    public static final String MAX_BIT_PRECISION = "max_bit_precision";
    public static final String MIN_BIT_PRECISION = "min_bit_precision";
    public static final String TABLE_NAME = "table_name";
    public static final String SECONDARY_TABLE_NAME = "secondary_table_name";
    public static final String SPARK_CONF = "spark_conf";

    //balance
    public static final String DISTRIBUTION_PATH = "distribution_path";
    public static final String SERVER_COUNT = "server_count";
    public static final String BALANCE_TYPE = "balance_type";
    public static final String DATA_COUNT_BALANCE = "data_count_balance";
    public static final String CELL_COUNT_BALANCE = "cell_count_balance";
    public static final String UNIQUE_PATTERN = "unique_pattern";

}
