package edu.nju.pasalab.util

import edu.nju.pasalab.util.ConstrainedType.ConstrainedType
import edu.nju.pasalab.util.DepType.DepType
import edu.nju.pasalab.util.POSType.POSType
import edu.nju.pasalab.util.SyntaxType.SyntaxType


class ExtractionParameters  (var optType:ExtractType.Value) extends Serializable {
  /**
    * Translation Model
    */
  var inputFileName : String = ""
  var rootDir : String = ""
  var maxc = 5
  var maxe = 5
  var maxSpan = 10
  var maxInitPhraseLength = 10
  var maxSlots = 2
  var minCWords = 1
  var minEWords = 1

  var maxCSymbols = 5
  var maxESymbols = 5
  var minWordsInSlot = 1
  var allowConsecutive = false
  var allowAllUnaligned = false

  var c2eWord = "WordTranslationC2E"
  var e2cWord = "WordTranslationE2C"

  //withNullOption this parameter can be Set true if need
  var withNullOption= false
  var allowBoundaryUnaligned = true
  var cutoff = 1
  var direction = Direction.C2E

  var withSyntaxInfo : Boolean = false
  var depType:DepType = DepType.DepString
  var posType:POSType = POSType.AllPossible
  var constrainedType:ConstrainedType = ConstrainedType.WellFormed

  var syntax:SyntaxType = SyntaxType.S2D
  /**
    * two structure are considered as well-formed: floating and fixed
    * this option control whether to record the POS of the head of a floating structure , or use non-terminal X
    */
  var keepFloatingPOS = false

  /**
   * this parameters is used for smoothed
   */
  //val smooth = true
  var smoothedWeight : Float = 0.2f
  var allowGoodTuring = false
  var allowKN = false
  var allowMKN = false
  var allowSmoothing = false

  /**
    * Spark parameter settings
    */
  var partitionNum : Int = 0
  var encoding : String = "UTF-8"
  var totalCores : Int = 192
  var parallelism : Int = 192
  var driverMemory : Int = 40
  var driverCores : Int = 8
  var driverExtraJavaOptions  : String = ""
  var executorMemory : Int = 40
  var executorCores : Int = 8
  var executorNum : Int = 20
  var executorExtraJavaOptions  : String = ""

  var registrationRequired : Boolean = true

  var memoryFraction : Double = 0.6
  var memoryStorageFraction : Double = 0.5
  var memoryUseLegacyMode : Boolean = false
  var storageMemoryFraction : Double= 0.6
  var shuffleMemoryFraction : Double = 0.2
  var isOffHeap : Boolean = false
  var offHeapSize : String = ""
  var compressType : String = "org.apache.hadoop.io.compress.GzipCodec"
  /**
    * Word Alignment Model
    */
  var source : String = ""
  var target : String = ""
  var alignedRoot : String = ""
  var redo : Boolean = true
  // var partitionNum : Int = 0
  var splitNum : Int = 2
  var iterations : Int = 5
  var nCPUs : Int = 0
  var memoryLimit : Int = 0
  var numCls : Int = 0
  var maxSentLen : Int = 100
  var totalReducer : Int = 0
  var overwrite  : Boolean= true
  var nocopy : Boolean = false
  var filterLog : String = "filter-log"

  var srcClass : String = ""
  var tgtClass : String = ""

  var d4norm: String = ""
  var hmmnorm: String = ""
  var giza: String = ""
  var symalbin : String = ""
  var symalmethod : String = ""
  var symalscript : String = ""
  var giza2bal : String = ""

  var trainSeq : String = ""
  var verbose : Boolean = false
  /**
    * Language Model
    */

  var lmRootDir : String = ""
  var lmInputFile : String = "Monolingual corpus"
  var N : Int = 5
  var splitDataRation :Double = 0.0
  var sampleNum : Int =  0
  var expectedErrorRate : Double = 0.0
  var offset : Int = 100
  var GT : Boolean = false
  var KN : Boolean = false
  var MKN : Boolean = false
  var GSB : Boolean = false


}

/**
 * parameters about the target dependency structure, i.e. S2D
 * there are several modes for using target side dependency structure
 * 1: using the dependency string
 * 2: using the postag of each span only
 */
object DepType extends Enumeration{
  type DepType = Value
  val DepString,None = Value
}

/**
 * 1. AllPossible : aggregate all possible pos-tags for each span
 * 2. BestOne : use the pos-tag with the largest count
 * 3. Frame : use the pos-tag for the head of the span and the head of each sub spans as a frame
 * 4. None : do not use postag
 */
object POSType extends Enumeration{
  type POSType = Value
  val AllPossible, BestOne, Frame, None = Value
}

/**
 * and there are three levels of hard syntax constraint
 * 1: every span should be syntactically well-formed
 * 2: the span could be relaxed well-formed
 * 3: the span could be any continuous phrase
 */
object ConstrainedType extends Enumeration {
  type ConstrainedType = Value
  val WellFormed, Relaxed, ANY = Value
}

/**
 * T2S and S2T is not implemented yet
 */
object SyntaxType extends Enumeration{
  type SyntaxType = Value
  val S2S, T2S, S2T, S2D, NULL = Value
}

object Direction extends Enumeration{
  type Direction = Value
  val E2C, C2E, All = Value
}

object ExtractType extends Enumeration{
  type ExtractType = Value
  val Rule, Phrase, PhraseWithReordering = Value
}
object CompressType extends Enumeration {
  type CompressType = Value
  val LZ4, Bzip2, Gzip, Snappy, NONE = Value
}


