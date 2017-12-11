package edu.nju.pasalab.mt.optimize

import java.io.File

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.util.configRead
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by YWJ on 2017.5.11.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object testParquet {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")

    val configFile = "data/training.conf"
    val config = ConfigFactory.parseFile(new File(configFile))

    val ep = configRead.getTrainingSettings(config)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", ep.compressType)
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)

    spark.stop()
  }

  def convert(sqlContext: SQLContext, fileName: String, schema: StructType, tableName: String) {
    // import text-based table first into a data frame
    val df = sqlContext.read.format("com.databricks.spark.csv").
      schema(schema).option("delimiter", "|").load(fileName)
    // now simply write to a parquet file
    df.write.parquet("/user/spark/data/parquet/"+tableName)
  }

}
