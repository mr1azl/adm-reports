package com.adm.sparkserver.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Created by samklr on 15/08/15.
 */
public class SparkHiveRunner {

    private final static Logger LOGGER = LoggerFactory.getLogger(SparkHiveRunner.class);

    private Config config = ConfigFactory.load();
    private String data = config.getString("app.data.default");

    private static SparkContext sparkContext = SparkFactory.sc;
    private static HiveContext hiveContext = new HiveContext(sparkContext);


    static{

        hiveContext.setConf("spark.sql.shuffle.partitions", "4");
        hiveContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","40000");
        hiveContext.setConf("spark.sql.parquet.compression.codec","snappy");
        hiveContext.setConf("spark.sql.parquet.filterPushdown","true");
        hiveContext.setConf("spark.sql.codegen","true");

        hiveContext.sql("add jar libs/esri-geometry-api-1.2.1.jar");
        hiveContext.sql("add jar libs/spatial-sdk-json-1.1.1.jar");
        hiveContext.sql("add jar libs/spatial-sdk-hive-1.1.1.jar");

        hiveContext.sql("create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point' ");
        hiveContext.sql("create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains'");
        hiveContext.sql("create temporary function ST_Buffer as 'com.esri.hadoop.hive.ST_Buffer'");
        hiveContext.sql("create temporary function ST_LineString as 'com.esri.hadoop.hive.ST_LineString'");
        hiveContext.sql("create temporary function ST_Length as 'com.esri.hadoop.hive.ST_Length'");
        hiveContext.sql("create temporary function ST_GeodesicLengthWGS84 as 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84'");
        hiveContext.sql("create temporary function ST_SetSRID as 'com.esri.hadoop.hive.ST_SetSRID'");
        hiveContext.sql("create temporary function ST_Polygon as 'com.esri.hadoop.hive.ST_Polygon' ");
        hiveContext.sql("create temporary function ST_Intersects as 'com.esri.hadoop.hive.ST_Intersects'");
    }

    private DataFrame logsDataFrame;

    public SparkHiveRunner(SparkFactory sparkFactory) {
        this.init();
    }

    private void init(){

        LOGGER.info("Loading and Cleaning Log Data into Spark Warehouse");

        DataFrame df = hiveContext.read().option("mode","overwrite").parquet(data).cache();
        this.logsDataFrame = df.na().drop();

        Preconditions.checkNotNull(logsDataFrame, "LogsDataframe must not be null");
        logsDataFrame.printSchema();

        logsDataFrame.registerTempTable("log_days");
        hiveContext.cacheTable("log_days");
    }


    public DataFrame runHiveQuery(String hiveQueryString){
        Preconditions.checkNotNull(hiveQueryString, "Query String is null");
        return hiveContext.sql(hiveQueryString);
    }


    public static void main(String[] args){

        SparkHiveRunner hr= new SparkHiveRunner(new SparkFactory());

        long tStart = System.nanoTime();
        DataFrame geoQuery = hr.runHiveQuery(
                        " SELECT log_day, log_hour, COUNT(*) impressions " +
                        " FROM log_days " +
                        " GROUP BY log_day, log_hour ORDER BY log_hour"
        );
        geoQuery.show();
        long tEnd = System.nanoTime();

        //within(lat, lon, 10.0, 48.8436, 2.3238)
        //Query 2
        long tStart2 = System.nanoTime();
        DataFrame geoQuery2 = hr.runHiveQuery(
                        " SELECT log_day, log_hour, count(distinct userid) users, COUNT(*) impressions, sum(impflag) sum_impflags" +
                        " FROM log_days " +
                        " WHERE ST_Contains ((ST_BUFFER(ST_Point(2.3238,48.8436), 10)), ST_Point(lon, lat))" +
                        " GROUP BY log_day, log_hour " +
                        " ORDER BY log_hour "   );
        geoQuery2.show();

        long tEnd2 = System.nanoTime();


        long tStart3 = System.nanoTime();
/**
        String query3 =
                "SELECT pubuid, log_day, log_hour, count(distinct userid) users, COUNT(*) impressions, sum(impflag) impflags " +
                "FROM log_days " +
                "WHERE within(lat, lon, 10.0, 48.8436, 2.3238) " +
                "GROUP BY pubuid, log_day, log_hour";
        **/

        DataFrame geoQuery3 = hr.runHiveQuery(
                "SELECT pubuid, log_day, log_hour, count(distinct userid) users, COUNT(*) impressions, sum(impflag) impflags " +
                "FROM log_days " +
                "WHERE ST_Contains ((ST_BUFFER(ST_Point(2.3238,48.8436), 10)), ST_Point(lon, lat))" +
                "GROUP BY pubuid, log_day, log_hour");

        geoQuery3.show();
        long tEnd3 = System.nanoTime();


        System.out.println(" ====================================== Results ===========================================");



        System.out.println("First Query  took : " + TimeUnit.NANOSECONDS.toSeconds(tEnd - tStart) +" seconds");

        System.out.println("Second Query took : " + TimeUnit.NANOSECONDS.toSeconds(tEnd2 - tStart2) +" seconds");

        System.out.println("Third Query  took : " + TimeUnit.NANOSECONDS.toSeconds(tEnd3 - tStart3) +" seconds");

    }
}
