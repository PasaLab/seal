import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by YWJ on 2017.6.1.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
object testHDFSBug {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils")


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    val fs : FileSystem = FileSystem.get(new Configuration())

    if (fs.exists(new Path("data\\phrase\\dCorpus"))) {
      val data = spark.sparkContext.textFile("data\\phrase\\dCorpus")

      println(data.take(1).mkString(""))
    }


    spark.stop()
  }
}
