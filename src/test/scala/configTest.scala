import java.io.File

import com.typesafe.config.{ConfigRenderOptions, ConfigFactory, Config}
import edu.nju.pasalab.util._
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
/**
  * Created by YWJ on 2016.12.26.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object configTest {
  val logger  = LoggerFactory.getLogger(configTest.getClass)
  logger.info("*******************************************************")
  def main(args: Array[String]) {
    val config : Config = ConfigFactory.parseFile(new File("data/training.conf"))
    //val config : Config = ConfigFactory.load()
    //val str = config.root()
    //str.foreach(println)
    val ep = getTrainingSettings(config)
    printTrainingSetting(ep)
  }

  def getTrainingSettings (config : Config) : ExtractionParameters = {
    val ep = new ExtractionParameters(ExtractType.Phrase)
    config.resolve().getString("mt.optType") match {
      case "Rule" => ep.optType = ExtractType.Rule
      case "PhraseWithReordering" => ep.optType = ExtractType.PhraseWithReordering
      case _ => ep.optType = ExtractType.Phrase
    }
    ep.inputFileName = config.getString("mt.inputDir")
    ep.rootDir = config.getString("mt.rootDir")
    ep.partitionNum = config.getInt("mt.partitionNum")
    ep.maxc = config.getInt("mt.maxc")
    ep.maxe = config.getInt("mt.maxe")
    ep.maxSpan = config.getInt("mt.maxSpan")
    ep.maxInitPhraseLength = config.getInt("mt.maxInitPhraseLength")
    ep.maxSlots = config.getInt("mt.maxSlots")
    ep.minCWords = config.getInt("mt.minCWords")
    ep.minEWords = config.getInt("mt.minEWords")

    ep.maxCSymbols = config.getInt("mt.maxCSymbols")
    ep.maxESymbols = config.getInt("mt.maxESymbols")
    ep.minWordsInSlot = config.getInt("mt.minWordsInSlot")

    //withNullOption this parameter can be Set true if need
    ep.withNullOption = config.getBoolean("mt.withNullOption")
    ep.allowBoundaryUnaligned = config.getBoolean("mt.allowBoundaryUnaligned")

    //model pruning setting
    ep.cutoff = config.getInt("mt.cutoff")

    config.resolve().getString("mt.direction") match {
      case "C2E" => ep.direction = Direction.C2E
      case "E2C" => ep.direction = Direction.E2C
      case _ => ep.direction = Direction.All
    }

    /**
      * Smoothing parameter setting
      */
    ep.smoothedWeight = config.getDouble("mt.smoothedWeight").toFloat
    ep.allowGoodTuring = config.getBoolean("mt.allowGoodTuring")
    ep.allowKN = config.getBoolean("mt.allowKN")
    ep.allowMKN = config.getBoolean("mt.allowMKN")
    ep.allowSmoothing = config.getBoolean("mt.allowSmoothing")

    /**
      * Syntax Setting
      */
    ep.withSyntaxInfo = config.getBoolean("mt.withSyntaxInfo")
    ep.keepFloatingPOS = config.getBoolean("mt.keepFloatingPOS")
    config.resolve().getString("mt.depType") match {
      case "DepString" => ep.depType = DepType.DepString
      case _ => ep.depType = DepType.None
    }
    config.resolve().getString("mt.posType") match {
      case "AllPossible" => ep.posType = POSType.AllPossible
      case "BestOne" => ep.posType = POSType.BestOne
      case "Frame" => ep.posType = POSType.Frame
      case _ => ep.posType = POSType.None
    }
    config.resolve().getString("mt.constrainedType") match  {
      case "WellFormed" => ep.constrainedType = ConstrainedType.WellFormed
      case "Relaxed" => ep.constrainedType = ConstrainedType.Relaxed
      case  _ => ep.constrainedType = ConstrainedType.ANY
    }
    config.resolve().getString("mt.syntax") match {
      case "S2D" => ep.syntax = SyntaxType.S2D
      case "S2S" => ep.syntax = SyntaxType.S2S
      case "S2T" => ep.syntax = SyntaxType.S2T
      case "T2S" => ep.syntax = SyntaxType.T2S
      case _ => ep.syntax = SyntaxType.NULL
    }

    /**
      * Common & Hardware Parameter
      */
    ep.encoding = config.getString("wam.encoding")
    ep.totalCores = config.getInt("sparkPara.totalCores")
    ep.parallelism = config.getInt("sparkPara.parallelism")
    ep.registrationRequired = config.getBoolean("sparkPara.registrationRequired")
    ep.driverMemory = config.getInt("sparkPara.driver.memory")
    ep.driverCores = config.getInt("sparkPara.driver.cores")
    ep.driverExtraJavaOptions = config.getString("sparkPara.driver.extraJavaOptions")
    ep.executorMemory = config.getInt("sparkPara.executor.memory")
    ep.executorNum = config.getInt("sparkPara.executor.num")
    ep.executorCores = config.getInt("sparkPara.executor.cores")
    ep.executorExtraJavaOptions = config.getString("sparkPara.executor.extraJavaOptions")

    ep.memoryFraction = config.getDouble("sparkPara.memoryManagement.memory.fraction")
    ep.memoryStorageFraction = config.getDouble("sparkPara.memoryManagement.memory.storageFraction")
    ep.memoryUseLegacyMode = config.getBoolean("sparkPara.memoryManagement.memory.useLegacyMode")
    ep.storageMemoryFraction = config.getDouble("sparkPara.memoryManagement.storage.memoryFraction")
    ep.shuffleMemoryFraction = config.getDouble("sparkPara.memoryManagement.shuffle.memoryFraction")

    /**
      * Word Alignment Model
      */
    ep.source = config.getString("wam.source")
    ep.target = config.getString("wam.target")
    ep.alignedRoot = config.getString("wam.alignedRoot")
    ep.redo = config.getBoolean("wam.redo")
    ep.iterations = config.getInt("wam.iterations")
    ep.nCPUs = config.getInt("wam.nCPUs")
    ep.memoryLimit = config.getInt("wam.memoryLimit")
    ep.numCls = config.getInt("wam.numCls")
    ep.maxSentLen = config.getInt("wam.maxSentLen")
    ep.totalReducer = config.getInt("wam.totalReducer")
    ep.verbose = config.getBoolean("wam.verbose")
    ep.overwrite = config.getBoolean("wam.overwrite")
    ep.nocopy = config.getBoolean("wam.nocopy")
    ep.splitNum = config.getInt("wam.splitNum")

    ep.trainSeq = config.getString("wam.trainSeq")
    ep.d4norm = config.getString("wam.d4norm")
    ep.hmmnorm = config.getString("wam.hmmnorm")
    ep.giza = config.getString("wam.giza")
    ep.symalbin = config.getString("wam.symalbin")
    ep.symalmethod = config.getString("wam.symalmethod")
    ep.symalscript = config.getString("wam.symalscript")
    ep.giza2bal = config.getString("wam.giza2bal")

    ep.filterLog = config.getString("wam.filterlog")
    ep.srcClass = config.getString("wam.srcClass")
    ep.tgtClass = config.getString("wam.tgtClass")

    /**
      * Language Model
      */
    ep.lmInputFile = config.getString("lm.inputFileName")
    ep.N = config.getInt("lm.N")
    ep.splitDataRation = config.getDouble("lm.splitDataRation")
    ep.sampleNum = config.getInt("lm.sampleNum")
    ep.expectedErrorRate = config.getDouble("lm.expectedErrorRate")
    ep.offset = config.getInt("lm.offset")
    ep.GT = config.getBoolean("lm.GT")
    ep.KN = config.getBoolean("lm.KN")
    ep.MKN = config.getBoolean("lm.MKN")
    ep.GSB = config.getBoolean("lm.GSB")

    ep
  }

  def printTrainingSetting(ep : ExtractionParameters): Unit = {
    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Common & Hardware Parameter Settings -------------------------")
    printf("--encoding %50s\n", ep.encoding)
    printf("--partitionNum %44d\n", ep.partitionNum)
    printf("--total CPU Cores %41d\n", ep.totalCores)
    printf("--spark.default.parallelism %31d\n", ep.parallelism)
    printf("spark.kryo.registrationRequired %28b\n", ep.registrationRequired)
    printf("--spark.driver.memory %36d\n", ep.driverMemory)
    printf("--spark.driver.cores %36d\n", ep.driverCores)
    printf("--spark.driver.extraJavaOption %66s\n", ep.driverExtraJavaOptions)
    printf("--spark.executor.memory %34d\n", ep.executorMemory)
    printf("--spark.executor.num %37d\n", ep.executorNum)
    printf("--spark.executor.cores %34d\n", ep.executorCores)
    printf("--spark.executor.extraJavaOptions %63s\n", ep.executorExtraJavaOptions)
    printf("--spark.memory.fraction %40f\n", ep.memoryFraction)
    printf("--spark.memory.storageFraction %33f\n", ep.memoryStorageFraction)
    printf("--spark.memory.useLegacyMode %32b\n", ep.memoryUseLegacyMode)
    printf("--spark.storage.memoryFraction %33f\n", ep.storageMemoryFraction)
    printf("--spark.shuffle.memoryFraction %33f\n", ep.shuffleMemoryFraction)
    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Word Alignment Model Parameter Settings ------------------------")
    printf("--source %63s\n", ep.source)
    printf("--target %63s\n", ep.target)
    printf("--alignedRoot %50s\n", ep.alignedRoot)
    printf("--redo %52b\n", ep.redo)
    printf("--iteration %44d\n", ep.iterations)
    printf("--nCPUs %48d\n", ep.nCPUs)
    printf("--memoryLimit %43d\n", ep.memoryLimit)
    printf("--numCls %48d\n", ep.numCls)
    printf("--maxSentLen %45d\n", ep.maxSentLen)
    printf("--totalReducer %42d\n", ep.totalReducer)
    printf("--splitNum %46d\n", ep.splitNum)
    printf("--overwrite %47b\n", ep.overwrite)
    printf("--nocopy %51b\n", ep.nocopy)
    printf("--verbose %50b\n", ep.verbose)
    printf("--filterLog %53s\n", ep.filterLog)
    printf("--srcClass %50s\n", ep.srcClass)
    printf("--tgtClass %50s\n", ep.tgtClass)
    printf("--trainSeq %50s\n", ep.trainSeq)
    printf("--d4norm %52s\n", ep.d4norm)
    printf("--hmmnorm %52s\n", ep.hmmnorm)
    printf("--giza %53s\n", ep.giza)
    printf("--symalbin %49s\n", ep.symalbin)
    printf("--symalmethod %60s\n", ep.symalmethod)
    printf("--symalscript %49s\n", ep.symalscript)
    printf("--giza2pal %55s\n", ep.giza2bal)

    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Translation Model Parameter Settings ---------------------------")
    printf("--inputFileName %59s\n", ep.inputFileName)
    printf("--rootDir %55s\n", ep.rootDir)
    printf("--ExtractType%47s\n", ep.optType)
    printf("--direction %47s\n", ep.direction)
    printf("--maxC %50d\n", ep.maxc)
    printf("--maxE %50d\n", ep.maxe)
    printf("--maxSpan%49d\n", ep.maxSpan)
    printf("--maxInitPhraseLength %36d\n", ep.maxInitPhraseLength)
    printf("--maxSlots %46d\n", ep.maxSlots)
    printf("--minCWords %45d\n", ep.minCWords)
    printf("--minEWords %45d\n", ep.minEWords)
    printf("--maxCSymbols %43d\n", ep.maxCSymbols)
    printf("--maxESymbols %43d\n", ep.maxESymbols)
    printf("--minWordsInSlot %40d\n", ep.minWordsInSlot)
    printf("--allowConsecutive %42b\n", ep.allowConsecutive)
    printf("--allowAllUnaligned %41b\n", ep.allowAllUnaligned)
    printf("--c2eWord %64s\n", ep.c2eWord)
    printf("--c2eWord %64s\n", ep.e2cWord)
    printf("--cutoff %48d\n", ep.cutoff)
    printf("--withNullOption %44b\n", ep.withNullOption)
    printf("--allowBoundaryUnaligned %35b\n", ep.allowBoundaryUnaligned)
    printf("--allowSmoothing %44b\n", ep.allowSmoothing)
    printf("--smoothedWeight %47f\n", ep.smoothedWeight)
    printf("--allowGoodTuring %43b\n", ep.allowGoodTuring)
    printf("--allowKN %51b\n", ep.allowKN)
    printf("--allowMKN %50b\n", ep.allowMKN)
    printf("--withSyntaxInfo %43b\n", ep.withSyntaxInfo)
    printf("--syntax %50s\n", ep.syntax)
    printf("--depType %55s\n", ep.depType)
    printf("--posType %57s\n", ep.posType)
    printf("--constrainedType %48s\n", ep.constrainedType)
    printf("--keepFloatingPOS %43b\n", ep.keepFloatingPOS)
    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Language Model Parameter Settings ---------------------------")
    printf("--lmInputFileName %51s\n", ep.lmInputFile)
    printf("--N %53d\n", ep.N)
    printf("--splitDataRation %46f\n", ep.splitDataRation)
    printf("--sampleNum %46d\n", ep.sampleNum)
    printf("--expectedErrorRate %44f\n", ep.expectedErrorRate)
    printf("--offset %50d\n", ep.offset)
    printf("--GT %55b\n", ep.GT)
    printf("--KN %56b\n", ep.KN)
    printf("--MKN %55b\n", ep.MKN)
    printf("--GSB %55b\n", ep.GSB)
    println("--------------------------------------------------------------------------------------------\n")
  }
}


