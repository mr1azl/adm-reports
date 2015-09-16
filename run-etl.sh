#! /bin/bash

mkdir /tmp/spark

spark-submit --class com.adm.spark.AdmEtl --master "local[*]" target/spark-server-0.1-SNAPSHOT.jar
