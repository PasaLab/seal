/*
package edu.nju.pasalab.mt.wordAlignment.compo

import java.io.{BufferedReader, InputStreamReader}

import gnu.trove.map.hash.{TObjectDoubleHashMap, TLongDoubleHashMap, TIntDoubleHashMap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.collection.mutable.HashMap


object ReadFromHDFS {

  def readTranslationTable_hmm(path: String): TLongDoubleHashMap = {
    var transMap: TLongDoubleHashMap = new TLongDoubleHashMap()
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              transMap.put(split_line(0).toLong ,split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    transMap
  }

  def readTranslationTable_trove(path: String): TObjectDoubleHashMap[String] = {
    var transMap: TObjectDoubleHashMap[String] = new TObjectDoubleHashMap[String]
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              transMap.put(split_line(0),split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    transMap
  }

  def readTranslationTable(path: String): HashMap[String, Double] = {
    var transMap: HashMap[String, Double] = new HashMap[String, Double]
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              transMap += (split_line(0) -> split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    transMap
  }

  def writeTranslationTableToRedis(path: String,pipeline:Pipeline) = {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              pipeline.set(split_line(0),split_line(1))
            }
          }
        }
        bReader.close
      }
    }
    pipeline.sync()
  }

  def readTranslationTable_trove(path: String,transMap:TObjectDoubleHashMap[String]) = {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              transMap.put(split_line(0),split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
  }


  def readTranslationTable_onedimension(path: String): TLongDoubleHashMap = {
    var transMap: TLongDoubleHashMap = new TLongDoubleHashMap()
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              transMap.put(split_line(0).toLong ,split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    transMap
  }

  def readAlignmentTable(path:String):HashMap[String,Double]={
    val alignmentTable = new mutable.HashMap[String,Double]()
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              alignmentTable += (split_line(0) -> split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    alignmentTable
  }

  def readAlignmentTable_trove(path:String,alignmentTable:TObjectDoubleHashMap[String])={
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              alignmentTable.put(split_line(0),split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
  }

  def readInitState(path:String):Array[Double]={
    val initState:HashMap[Int,Double] = new HashMap[Int,Double]()
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if (split_line.length == 2) {
              initState += (split_line(0).toInt -> split_line(1).toDouble)
            }
          }
        }
        bReader.close
      }
    }
    
    val initState_toArray = Array.ofDim[Double](initState.size)
    for(kv<-initState){
       initState_toArray(kv._1)=kv._2
    }
    initState_toArray
    
  }

  def readTransitionTable_onedimension(path: String):Array[Double] = {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    val transitionMap: TIntDoubleHashMap= new TIntDoubleHashMap()
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim
          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            val index = split_line(0).toInt
            val prob = split_line(1).toDouble
            transitionMap.put(index,prob)
          }
        }
      }
    }
    val transitionArray = Array.ofDim[Double](transitionMap.size())
    for(key<-transitionMap.keys())
      transitionArray(key)=transitionMap.get(key)
    transitionArray
  }

//  def readTransitionTable_onedimension(path: String):TIntDoubleHashMap = {
//    val conf: Configuration = new Configuration
//    val fs: FileSystem = FileSystem.get(conf)
//    val transitionMap: TIntDoubleHashMap= new TIntDoubleHashMap()
//    val pt: Path = new Path(path)
//    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
//    for (i <- 0 until fileCount) {
//      val fileName = path + "/part-" + f"$i%05d"
//      if (fs.exists(new Path(fileName))) {
//        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
//        while (bReader.ready) {
//          var line: String = bReader.readLine
//          line = line.trim
//          if (!line.isEmpty) {
//            val split_line: Array[String] = line.split("\\t")
//            val index = split_line(0).toInt
//            val prob = split_line(1).toDouble
//            transitionMap.put(index,prob)
//          }
//        }
//      }
//    }
//    transitionMap
//  }

  def readTransitionTable(path: String):Array[Array[Double]] = {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)
    var transitionArray: Array[Array[Double]] = null
    var flag = false
    val pt: Path = new Path(path)
    val fileCount = fs.getContentSummary(pt).getFileCount.toInt
    for (i <- 0 until fileCount) {
      val fileName = path + "/part-" + f"$i%05d"
      if (fs.exists(new Path(fileName))) {
        val bReader: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))))
        while (bReader.ready) {
          var line: String = bReader.readLine
          line = line.trim

          if (!line.isEmpty) {
            val split_line: Array[String] = line.split("\\t")
            if(!flag){
              transitionArray = Array.ofDim[Double](split_line.length-1, split_line.length-1)
              flag=true
            }
            val index = split_line(0).toInt
            for (j <- 1 until split_line.length)
              transitionArray(index)(j - 1) = split_line(j).toDouble
          }
        }
      }
    }
    transitionArray
  }
}
*/
