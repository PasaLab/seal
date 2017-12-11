package edu.nju.pasalab.util

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/**
  * Created by YWJ on 2016.12.26.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */
object configRead {
  val logger  = LoggerFactory.getLogger(configRead.getClass)

  def getTrainingSettings (config : Config) : ExtractionParameters = {
    /** Translation Model */

    val ep = new ExtractionParameters(ExtractType.Phrase)

    config.resolve().getString("mt.optType") match {
      case "Rule" => ep.optType = ExtractType.Rule
      case "PhraseWithReordering" => ep.optType = ExtractType.PhraseWithReordering
      case _ => ep.optType = ExtractType.Phrase
    }
    ep.inputFileName = config.getString("mt.inputDir")
    ep.rootDir = config.getString("mt.rootDir")
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

    /** Smoothing parameter setting */
    ep.smoothedWeight = config.getDouble("mt.smoothedWeight").toFloat
    ep.allowGoodTuring = config.getBoolean("mt.allowGoodTuring")
    ep.allowKN = config.getBoolean("mt.allowKN")
    ep.allowMKN = config.getBoolean("mt.allowMKN")
    ep.allowSmoothing = config.getBoolean("mt.allowSmoothing")

    /** Syntax Setting */
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


    /** Common & Hardware Parameter */
    ep.partitionNum = config.getInt("sparkPara.partitionNum")
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
    ep.isOffHeap = config.getBoolean("sparkPara.memoryManagement.offHeap.enabled")
    ep.offHeapSize = config.getString("sparkPara.memoryManagement.offHeap.size")
    ep.compressType = config.getString("sparkPara.IOCompress")

    /** Word Alignment Model */
    ep.source = config.getString("wam.source")
    ep.target = config.getString("wam.target")
    ep.alignedRoot = config.getString("wam.alignedRoot")
    ep.redo = config.getBoolean("wam.redo")
    ep.iterations = config.getInt("wam.iterations")
    ep.nCPUs = config.getInt("wam.nCPUs")
    ep.splitNum = config.getInt("wam.splitNum")
    ep.memoryLimit = config.getInt("wam.memoryLimit")
    ep.numCls = config.getInt("wam.numCls")
    ep.maxSentLen = config.getInt("wam.maxSentLen")
    ep.totalReducer = config.getInt("wam.totalReducer")
    ep.verbose = config.getBoolean("wam.verbose")
    ep.overwrite = config.getBoolean("wam.overwrite")
    ep.nocopy = config.getBoolean("wam.nocopy")

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

    /** Language Model */
    ep.lmRootDir = config.getString("lm.lmRootDir")
    ep.lmInputFile = config.getString("lm.lmInputFileName")
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
    printf("--encoding %52s\n", ep.encoding)
    printf("--partitionNum %44d\n", ep.partitionNum)
    printf("--total CPU Cores %41d\n", ep.totalCores)
    printf("--spark.default.parallelism %31d\n", ep.parallelism)
    printf("--spark.kryo.registrationRequired %28b\n", ep.registrationRequired)
    printf("--spark.driver.memory %38d\n", ep.driverMemory)
    printf("--spark.driver.cores %38d\n", ep.driverCores)
    printf("--spark.driver.extraJavaOption %68s\n", ep.driverExtraJavaOptions)
    printf("--spark.executor.memory %36d\n", ep.executorMemory)
    printf("--spark.executor.num %39d\n", ep.executorNum)
    printf("--spark.executor.cores %36d\n", ep.executorCores)
    printf("--spark.executor.extraJavaOptions %114s\n", ep.executorExtraJavaOptions)
    printf("--spark.memory.fraction %42f\n", ep.memoryFraction)
    printf("--spark.memory.storageFraction %35f\n", ep.memoryStorageFraction)
    printf("--spark.memory.useLegacyMode %34b\n", ep.memoryUseLegacyMode)
    printf("--spark.storage.memoryFraction %35f\n", ep.storageMemoryFraction)
    printf("--spark.shuffle.memoryFraction %35f\n", ep.shuffleMemoryFraction)
    printf("--spark.memory.offHeap.enabled %32b\n", ep.isOffHeap)
    printf("--spark.memory.offHeap.size %34s\n", ep.offHeapSize)
    printf("--spark storage compress type %67s\n", ep.compressType)
    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Word Alignment Model Parameter Settings ------------------------")
    printf("--source %93s\n", ep.source)
    printf("--target %93s\n", ep.target)
    printf("--alignedRoot %80s\n", ep.alignedRoot)
    printf("--redo %52b\n", ep.redo)
    printf("--iteration %44d\n", ep.iterations)
    printf("--nCPUs %48d\n", ep.nCPUs)
    printf("--memoryLimit %43d\n", ep.memoryLimit)
    printf("--numCls %48d\n", ep.numCls)
    printf("--maxSentLen %45d\n", ep.maxSentLen)
    printf("--totalReducer %42d\n", ep.totalReducer)
    printf("--splitNum %45d\n", ep.splitNum)
    printf("--overwrite %47b\n", ep.overwrite)
    printf("--nocopy %51b\n", ep.nocopy)
    printf("--verbose %50b\n", ep.verbose)
    printf("--filterLog %53s\n", ep.filterLog)
    printf("--srcClass %50s\n", ep.srcClass)
    printf("--tgtClass %50s\n", ep.tgtClass)
    printf("--trainSeq %50s\n", ep.trainSeq)
    printf("--d4norm %93s\n", ep.d4norm)
    printf("--hmmnorm %93s\n", ep.hmmnorm)
    printf("--giza %94s\n", ep.giza)
    printf("--symalbin %90s\n", ep.symalbin)
    printf("--symalmethod %60s\n", ep.symalmethod)
    printf("--symalscript %91s\n", ep.symalscript)
    printf("--giza2pal %97s\n", ep.giza2bal)

    println("--------------------------------------------------------------------------------------------\n")
    println("----------------------------- Translation Model Parameter Settings ---------------------------")
    printf("--inputFileName %89s\n", ep.inputFileName)
    printf("--rootDir %95s\n", ep.rootDir)
    printf("--ExtractType%49s\n", ep.optType)
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
    printf("--inputFileName %54s\n", ep.lmInputFile)
    printf("--lm working root directory %36s\n", ep.lmRootDir)
    printf("--N %54d\n", ep.N)
    printf("--splitDataRation %47f\n", ep.splitDataRation)
    printf("--sampleNum %47d\n", ep.sampleNum)
    printf("--expectedErrorRate %45f\n", ep.expectedErrorRate)
    printf("--offset %51d\n", ep.offset)
    printf("--GT %57b\n", ep.GT)
    printf("--KN %57b\n", ep.KN)
    printf("--MKN %56b\n", ep.MKN)
    printf("--GSB %55b\n", ep.GSB)
    println("--------------------------------------------------------------------------------------------\n")

  }

  def printIntoLog(ep : ExtractionParameters): Unit = {

    logger.info("--------------------------------------------------------------------------------------------\n")
    logger.info("----------------------------- Common & Hardware Parameter Settings -------------------------")
    logger.info(f"--encoding ${ep.encoding}%52s")
    logger.info(f"--partitionNum ${ep.partitionNum}%44d")
    logger.info(f"--total CPU Cores ${ep.totalCores}%41d")
    logger.info(f"--spark.default.parallelism ${ep.parallelism}%31d")
    logger.info(f"--spark.kryo.registrationRequired ${ep.registrationRequired}%28b")
    logger.info(f"--spark.driver.memory ${ep.driverMemory}%38d")
    logger.info(f"--spark.driver.cores ${ep.driverCores}%38d")
    logger.info(f"--spark.driver.extraJavaOption ${ep.driverExtraJavaOptions}%68s")
    logger.info(f"--spark.executor.memory ${ep.executorMemory}%36d")
    logger.info(f"--spark.executor.num ${ep.executorNum}%39d")
    logger.info(f"--spark.executor.cores ${ep.executorCores}%36d")
    logger.info(f"--spark.executor.extraJavaOptions ${ep.executorExtraJavaOptions}%114s")
    logger.info(f"--spark.memory.fraction ${ep.memoryFraction}%42f")
    logger.info(f"--spark.memory.storageFraction ${ep.memoryStorageFraction}%35f")
    logger.info(f"--spark.memory.useLegacyMode ${ep.memoryUseLegacyMode}%34b")
    logger.info(f"--spark.storage.memoryFraction ${ep.storageMemoryFraction}%35f")
    logger.info(f"--spark.shuffle.memoryFraction ${ep.shuffleMemoryFraction}%35f")
    logger.info(f"--spark.memory.offHeap.enabled ${ep.isOffHeap}%32b")
    logger.info(f"--spark.memory.offHeap.size ${ep.offHeapSize}%34s")
    logger.info(f"--spark storage compress type ${ep.compressType}%67s")
    logger.info("--------------------------------------------------------------------------------------------\n")
    logger.info("----------------------------- Word Alignment Model Parameter Settings ------------------------")
    logger.info(f"--source ${ep.source}%93s")
    logger.info(f"--target ${ep.target}%93s")
    logger.info(f"--alignedRoot ${ep.alignedRoot}%80s")
    logger.info(f"--redo ${ep.redo}%52b")
    logger.info(f"--iteration ${ep.iterations}%44d")
    logger.info(f"--nCPUs ${ep.nCPUs}%48d")
    logger.info(f"--memoryLimit ${ep.memoryLimit}%43d")
    logger.info(f"--numCls ${ep.numCls}%48d")
    logger.info(f"--maxSentLen ${ep.maxSentLen}%45d")
    logger.info(f"--totalReducer ${ep.totalReducer}%42d")
    logger.info(f"--splitNum ${ep.splitNum}%46d")
    logger.info(f"--overwrite ${ep.overwrite}%47b")
    logger.info(f"--nocopy ${ep.nocopy}%51b")
    logger.info(f"--verbose ${ep.verbose}%50b")
    logger.info(f"--filterLog ${ep.filterLog}%53s")
    logger.info(f"--srcClass ${ep.srcClass}%50s")
    logger.info(f"--tgtClass ${ep.tgtClass}%50s")
    logger.info(f"--trainSeq ${ep.trainSeq}%50s")
    logger.info(f"--d4norm ${ep.d4norm}%93s")
    logger.info(f"--hmmnorm ${ep.hmmnorm}%93s")
    logger.info(f"--giza ${ep.giza}%94s")
    logger.info(f"--symalbin ${ep.symalbin}%90s")
    logger.info(f"--symalmethod ${ep.symalmethod}%60s")
    logger.info(f"--symalscript ${ep.symalscript}%91s")
    logger.info(f"--giza2pal ${ep.giza2bal}%97s")

    logger.info("--------------------------------------------------------------------------------------------\n")
    logger.info("----------------------------- Translation Model Parameter Settings ---------------------------")
    logger.info(f"--inputFileName ${ep.inputFileName}%89s")
    logger.info(f"--rootDir ${ep.rootDir}%95s")
    logger.info(f"--ExtractType${ep.optType}%49s")
    logger.info(f"--direction ${ep.direction}%47s")
    logger.info(f"--maxC ${ep.maxc}%50d")
    logger.info(f"--maxE ${ep.maxe}%50d")
    logger.info(f"--maxSpan${ep.maxSpan}%49d")
    logger.info(f"--maxInitPhraseLength ${ep.maxInitPhraseLength}%36d")
    logger.info(f"--maxSlots ${ep.maxSlots}%46d")
    logger.info(f"--minCWords ${ep.minCWords}%45d")
    logger.info(f"--minEWords ${ep.minEWords}%45d")
    logger.info(f"--maxCSymbols ${ep.maxCSymbols}%43d")
    logger.info(f"--maxESymbols ${ep.maxESymbols}%43d")
    logger.info(f"--minWordsInSlot ${ep.minWordsInSlot}%40d")
    logger.info(f"--allowConsecutive ${ep.allowConsecutive}%42b")
    logger.info(f"--allowAllUnaligned ${ep.allowAllUnaligned}%41b")
    logger.info(f"--c2eWord ${ep.c2eWord}%64s")
    logger.info(f"--c2eWord ${ep.e2cWord}%64s")
    logger.info(f"--cutoff ${ep.cutoff}%48d")
    logger.info(f"--withNullOption ${ep.withNullOption}%44b")
    logger.info(f"--allowBoundaryUnaligned ${ep.allowBoundaryUnaligned}%35b")
    logger.info(f"--allowSmoothing ${ep.allowSmoothing}%44b")
    logger.info(f"--smoothedWeight ${ep.smoothedWeight}%47f")
    logger.info(f"--allowGoodTuring ${ep.allowGoodTuring}%43b")
    logger.info(f"--allowKN ${ep.allowKN}%51b")
    logger.info(f"--allowMKN ${ep.allowMKN}%50b")
    logger.info(f"--withSyntaxInfo ${ep.withSyntaxInfo}%43b")
    logger.info(f"--syntax ${ep.syntax}%50s")
    logger.info(f"--depType ${ep.depType}%55s")
    logger.info(f"--posType ${ep.posType}%57s")
    logger.info(f"--constrainedType ${ep.constrainedType}%48s")
    logger.info(f"--keepFloatingPOS ${ep.keepFloatingPOS}%43b")

    logger.info("--------------------------------------------------------------------------------------------\n")
    logger.info("----------------------------- Language Model Parameter Settings ---------------------------")
    logger.info(f"--inputFileName ${ep.lmInputFile}%54s")
    logger.info(f"--lm working root directory ${ep.lmRootDir}%36s")
    logger.info(f"--N ${ep.N}%54d")
    logger.info(f"--splitDataRation ${ep.splitDataRation}%47f")
    logger.info(f"--sampleNum ${ep.sampleNum}%47d")
    logger.info(f"--expectedErrorRate ${ep.expectedErrorRate}%45f")
    logger.info(f"--offset ${ep.offset}%51d")
    logger.info(f"--GT ${ep.GT}%57b")
    logger.info(f"--KN ${ep.KN}%57b")
    logger.info(f"--MKN ${ep.MKN}%56b")
    logger.info(f"--GSB ${ep.GSB}%55b")
    logger.info("--------------------------------------------------------------------------------------------\n")
  }
}
