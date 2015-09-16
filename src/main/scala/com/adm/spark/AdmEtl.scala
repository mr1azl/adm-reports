package com.adm.spark

import com.spatial4j.core.io.GeohashUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.functions._

import org.apache.spark.{SparkConf, SparkContext}



object AdmEtl extends App {

  val config = ConfigFactory.load()

  //val input = config.getString("app.parquet.input")    //
  val input = "/home/samklr/code/latticeiq/adm_data/raw_json/*"

  val output = "/home/samklr/code/latticeiq/adm_data/parquet.3M.logs" //


  val conf = new SparkConf()
    .setAppName("Parquet ETL")
    .setMaster(config.getString("app.spark.master"))
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.executor.memory", config.getString("app.spark.executor.memory"))
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.speculation","true")
    .set("spark.executor.extraJavaOptions"," -XX:+UseG1GC")
    .set("spark.broadcast.blockSize","8m")
    .set("spark.default.parallelism","4")

  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)

  sqlContext.setConf("spark.sql.shuffle.partitions", "4");
  sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","50000");
  sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
  sqlContext.setConf("spark.sql.parquet.filterPushdown","true")

  println(sparkContext.getConf.toDebugString)


  def mkPoint = udf( (a : Seq[Double]) => s"Point(${a(1)} ${a(0)})") // to Wkt
  //val logsWithWkt  = raw_data.withColumn("coords", mkPoint($"coordinates"))


  def mkString = udf((a: Seq[Double]) => a.mkString(", "))
  //val logsWithStringCoordinates = raw_data.withColumn("coord_string", mkString($"coordinates"))

  def mk2Columns =  udf((a: Seq[Double], i: Int) => a(i))

  def mkGeoHash = udf ((coords : Seq[Double] ) => GeohashUtils.encodeLatLon(coords(1), coords(0)))



  import sqlContext.implicits._

  //read the json data and remove all null fields
  val raw_data = sqlContext.read.format("json").load(input).na.drop
  
  println("Raw logs size: " +raw_data.count)

  //Create two columns latitude and longitude and remove the coordinates column
   val logs = raw_data.select( $"*", mk2Columns($"coordinates", lit(1)).alias("lat"),
                                     mk2Columns($"coordinates", lit(0)).alias("lon"),
                                     mkGeoHash($"coordinates").substr(0,6).alias("geohash"))
                      .drop("coordinates")
                      .orderBy("log_day")
                      .repartition(config.getInt("app.parquet.partitions") * 2)

   //Print Schema
   logs.printSchema
   //Show 5 elements of the new table
   logs.show()

  println("Transformed logs size: " +logs.count)

   //Now we save the created table as parquet files, partitioned by day and hour.
   logs.write.format("parquet")
             .mode(SaveMode.Overwrite)
             .partitionBy("log_day")
             .save(output)




}
