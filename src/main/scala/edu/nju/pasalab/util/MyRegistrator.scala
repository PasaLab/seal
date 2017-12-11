package edu.nju.pasalab.util

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo.{ScalaMutableMapSerializer, ScalaMutableSetSerializer}
import com.romix.scala.serialization.kryo.{ScalaCollectionSerializer, EnumerationSerializer}
import com.twitter.algebird.CMSInstance
import edu.nju.pasalab.mt.extraction.dataStructure._

import edu.nju.pasalab.mt.extraction.util.dependency.{DependencyNode, DependencyTree}
import edu.nju.pasalab.mt.extraction.util.smoothing.{GTSmoothing, KNSmoothing, MKNSmoothing}

import edu.nju.pasalab.mt.wordAlignment.programLogic
import edu.nju.pasalab.mt.wordAlignment.usemgiza.{NormalizeTask, TrainingSequence}
import edu.nju.pasalab.mt.wordAlignment.util.{ExternalUtils, GIZAAlignmentTask, HDFSDirInputStream}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.{Object2FloatOpenHashMap, Object2IntOpenHashMap}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import org.apache.spark.sql.types._
import scala.reflect.ClassTag

/**
  * Created by YWJ on 2017.1.5.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    kryo.register(classOf[TranslationUnit])
    kryo.register(classOf[DependencyNode])
    kryo.register(classOf[DependencyTree])
    kryo.register(classOf[ExtractionParameters])

    kryo.register(classOf[GTSmoothing])
    kryo.register(classOf[KNSmoothing])
    kryo.register(classOf[MKNSmoothing])

    kryo.register(classOf[Object2IntOpenHashMap[String]])
    kryo.register(classOf[Object2FloatOpenHashMap[String]])
    kryo.register(classOf[Int2ObjectOpenHashMap[String]])
    kryo.register(POSType.getClass)
    kryo.register(DepType.getClass)
    kryo.register(Class.forName("edu.nju.pasalab.util.ConstrainedType$"))
    kryo.register(SyntaxType.getClass)
    kryo.register(Direction.getClass)
    kryo.register(ExtractType.getClass)

    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.register(classOf[scala.Enumeration#Value])

    kryo.register(classOf[Array[String]])
    kryo.register(Class.forName("java.lang.Class"))
    kryo.register(Class.forName("java.util.HashMap"))

    kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$1"))
    kryo.register(scala.reflect.ManifestFactory.getClass)
    kryo.register(ClassTag.getClass)
    kryo.register(classOf[Object])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Int]])


    kryo.register(classOf[edu.nju.pasalab.mt.LanguageModel.Smoothing.GTSmoothing])
    kryo.register(classOf[edu.nju.pasalab.mt.LanguageModel.Smoothing.KNSmoothing])
    kryo.register(classOf[edu.nju.pasalab.mt.LanguageModel.Smoothing.MKNSmoothing])
   // kryo.register(classOf[edu.nju.pasalab.mt.LanguageModel.Smoothing.])

    kryo.register(ClassTag(Class.forName("org.apache.spark.util.collection.CompactBuffer")).wrap.runtimeClass)
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    //kryo.register(classOf[scala.reflect.ManifestFactory$$anon$1.class)

    // Serialization of Scala maps like Trees, etc
    kryo.addDefaultSerializer(classOf[scala.collection.Map[_,_]], classOf[ScalaMutableMapSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.generic.MapFactory[scala.collection.Map]], classOf[ScalaMutableMapSerializer])

    // Serialization of Scala sets
    kryo.addDefaultSerializer(classOf[scala.collection.Set[_]], classOf[ScalaMutableSetSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.generic.SetFactory[scala.collection.Set]], classOf[ScalaMutableSetSerializer])

    // Serialization of all Traversable Scala collections like Lists, Vectors, etc
    kryo.addDefaultSerializer(classOf[scala.collection.Traversable[_]], classOf[ScalaCollectionSerializer])

    kryo.register(classOf[TrainingSequence])
    kryo.register(classOf[GIZAAlignmentTask])
    kryo.register(classOf[NormalizeTask])
    kryo.register(classOf[ExternalUtils])
    kryo.register(classOf[HDFSDirInputStream])
    kryo.register(classOf[programLogic])

    kryo.register(classOf[StructType])
    kryo.register(classOf[Array[StructField]])
   // kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(classOf[ArrayType])
    kryo.register(classOf[LongType])
    kryo.register(classOf[BooleanType])
    kryo.register(classOf[ByteType])
    kryo.register(classOf[DoubleType])
    kryo.register(classOf[FloatType])
    kryo.register(classOf[IntegerType])
    kryo.register(classOf[MapType])
    kryo.register(classOf[StringType])
    kryo.register(classOf[NullType])
    kryo.register(classOf[ShortType])
    kryo.register(classOf[Metadata])

    kryo.register(classOf[InternalRow])
    kryo.register(classOf[Array[InternalRow]])
    kryo.register(classOf[UnsafeRow])
    kryo.register(classOf[Array[UnsafeRow]])
    kryo.register(Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"))
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))
    //https://docs.oracle.com/javase/6/docs/api/java/lang/Class.html#getName%28%29
    kryo.register(Class.forName("[[B"))

    kryo.register(Class.forName("com.twitter.algebird.CMSInstance"))
    kryo.register(Class.forName("com.twitter.algebird.CMSInstance$CountsTable"))
    kryo.register(Class.forName("com.twitter.algebird.CMSParams"))
    kryo.register(Class.forName("com.twitter.algebird.CMSHash"))
    kryo.register(Class.forName("com.twitter.algebird.CMSHasher$CMSHasherString$"))
  }
}
