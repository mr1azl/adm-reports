package com.adm.sparkserver.spark;

import com.adm.sparkserver.models.EffectiveQuery;
import com.adm.sparkserver.models.Query;
import com.adm.sparkserver.models.Result;
import com.adm.sparkserver.spatial.SpatialFilter;
import com.adm.sparkserver.utils.TimeCounter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO remove main method and push the content to unit tests
 */
public class SparkRunner implements Serializable{

    private final static Logger LOGGER = LoggerFactory.getLogger(SparkRunner.class);

    private static Config config = ConfigFactory.load();

    private static String data = config.getString("app.data.default");

    public static final JavaSparkContext sc = SparkFactory.jsc;

    public static final SQLContext sqx = SparkFactory.sqlContext;

    public static final DataFrame logsDF = sqx.read().parquet(data)
            .repartition(config.getInt("app.parquet.partitions"));

    public static void init(){

        sqx.registerDataFrameAsTable(logsDF, config.getString("app.tableName"));

        sqx.cacheTable(config.getString("app.tableName"));

        sqx.udf().register("within", new UDF5<Double, Double, Double, Double, Double, Boolean>() {
            @Override
            public Boolean call(Double lt, Double ln, Double rad, Double orginLt, Double orignLn) throws Exception {
                return SpatialFilter.within(lt, ln, rad, orginLt, orignLn);
            }
        }, DataTypes.BooleanType);


        LOGGER.info("Data Schema");

        logsDF.printSchema();

        LOGGER.warn(" Log size : " + logsDF.count());

    }

    public static List<Row> queryRunner(String sqlString){
      LOGGER.debug("Running Query " + sqlString);

        DataFrame df = sqx.sql(sqlString);

      return df.collectAsList();
    }


    public static long logCounter(){
        return  (logsDF != null) ? logsDF.count() : 0;
    }


    public static Result geoStats(EffectiveQuery query) throws JsonProcessingException {

        Map<Double, List<Row>> mapDay = new HashMap<>();
        Map<Double, List<Row>> mapPub = new HashMap<>();

        for (Double radius : query.getRadiuses()){

            DataFrame dfbyDay =  sqx.sql(
                            " SELECT log_day, count(DISTINCT userid) users, COUNT(*) impressions, SUM(impflag) " +
                            " FROM   log_days " +
                            " WHERE  ( log_day IN "+ query.timeFrameDateString() + " )" +
                            " AND    ( pubuid  IN "+ query.applistString()+ ")" +
                            " AND    within(lat, lon," + radius + ", " + query.getLat() + ", " + query.getLon() + " ) "+
                            " GROUP BY log_day ORDER BY log_day ");


            DataFrame dfByPub = sqx.sql(
                            " SELECT log_day, pubuid, COUNT(DISTINCT userid) users, COUNT(*) impressions, SUM(impflag) " +
                            " FROM   log_days " +
                            " WHERE  ( log_day IN " + query.timeFrameDateString() + " )" +
                            " AND    ( pubuid  IN " + query.applistString() + ")" +
                            " AND    within(lat, lon," + radius + ", " + query.getLat() + ", " + query.getLon() + " ) " +
                            " GROUP BY log_day, pubuid ORDER BY log_day");

            mapDay.put(radius, dfbyDay.collectAsList());
            mapPub.put(radius, dfByPub.collectAsList());
        }

        return new Result(mapDay, mapPub);
    }


    public static void main(String[] args) throws InterruptedException, JsonProcessingException {

        /**
         http://91.216.58.166/ws/geo-analytics.php?zone_latitude=48.858&zone_longitude=2.38115
         &radius=200000,3000,4000
         &applist=ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18
         &timeframe=2
         &schedule=11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111
         **/

        SparkRunner.init();

        TimeCounter tc = new TimeCounter();

        Query q = new Query ("48.858","2.38115", "200000,3000,4000", "ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18",3,"11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111");

        EffectiveQuery fq = new EffectiveQuery(q);

        Result results = geoStats(fq);

        System.out.println("Elapsed Time " + tc.elapsedTime());

        System.out.println(" stats by day : " + results.toJson());



    }

}
