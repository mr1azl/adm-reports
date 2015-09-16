package com.adm.sparkserver.spark;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

public class SparkFactory implements Serializable{

    private static Config config = ConfigFactory.load();
    
    public static SparkContext sc = new SparkContext(newSparkConf());
    public static JavaSparkContext jsc = new JavaSparkContext(sc);

    public static SQLContext sqlContext = new SQLContext(jsc);

    static {
        sqlContext.setConf("spark.sql.shuffle.partitions", "8");
        sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","50000");
        sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");
        sqlContext.setConf("spark.sql.parquet.filterPushdown","true");
        sqlContext.setConf("spark.sql.codegen","true");
    }


    private static SparkConf newSparkConf() {
        return new SparkConf()
                .setAppName(config.getString("app.name"))
                .setMaster(config.getString("app.spark.master"))
                .set("spark.executor.memory", config.getString("app.spark.executor.memory"))
                .set("spark.eventLog.enabled","false")
                .set("spark.driver.memory", config.getString("app.spark.driver.memory"))
                .set("spark.driver.maxResultSize", config.getString("app.spark.driver.maxResultSize"))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.kryo.registrator", MyKryoSerializer.class.getName())
                .set("spark.scheduler.mode", "FAIR")
                .set("spark.speculation", "true")
                .set("spark.broadcast.blockSize", "8m")
                .set("spark.shuffle.memoryFraction","0.4")
                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .set("spark.default.parallelism", "12") ;

    }


}
