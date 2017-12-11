

import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.objects.{ObjectArrayList, Object2IntOpenHashMap}

import reflect.runtime.universe._

/**
  * Created by YWJ on 2016.12.28.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
  */

object reflectTest {
  def main(args: Array[String]): Unit = {
    val da = new Array[Int](10).map(elem => 0)

    for (i <- da.indices) {
      println(da(i) + i)
    }
    val double = Container("abc")
    val map = Map(("a", 111), ("b", 2222))
    println(map.getOrElse("b", 3))
    println(map.getOrElse("c", 0))

    val map2 : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap[String]()
    map2.put("A", 1)
    map2.put("B", 3)
    map2.put("A", 2)
    map2.put("C", 4)

    val it = map2.keySet().iterator()
    var key : String = ""
    while (it.hasNext) {
      //val key = it.next()
      key = it.next()
      println("key " + key + " " + map2.getInt(key))
    }
    println(map2.getInt("D"))

    val arrList : ObjectArrayList[String] = new ObjectArrayList[String](3)
    arrList.add("ABC")
    arrList.add("Tom")
    arrList.add("Jack")
    arrList.set(0, "test")
    arrList.add("more")

    println(arrList)
    for (i <- 0 until arrList.size()) println(arrList.get(i))

    val sb = new StringBuilder
    sb.append("ABC")
    sb.append("Tom")
    if (sb.toString().contains("ABC")) println("Yes")
    //matchContainer(double)


    val arr : FloatArrayList = new FloatArrayList()
    arr.add(1.0f)
    arr.add(2.0f)
    for (i <- 0 until arr.size()) println(arr.getFloat(i))
  }

/*  import scala.reflect.Manifest
  def matchContainer[A: Manifest](c : Container[A]) = c match {
    case c : Container[String] if manifest[A]  <:< manifest[String] => println(c.value.toString.toUpperCase)
    case c : Container[Double] if manifest[A] <:< manifest[Double] => println(math.sqrt(c.value))
    case _ => println("_")
  }*/
/*  def matchContainer[A : TypeTag](c : Container[A]) = c match {
    case c : Container[String] if typeOf[A] <:< typeOf[String] => println(c.value)
    case c : Container[Double] if typeOf[A] <:< typeOf[Double] => println(c.value)
    case c : Container[_] => println("_")
  }*/
/*  def matchContainer[A : TypeTag](c : Container[A]) = c match {
    case c : Container[String] if c.value.isInstanceOf[String] => println(c.value.asInstanceOf[String].toUpperCase)
    case c : Container[Double] if c.value.isInstanceOf[Double] => println(math.sqrt(c.value.asInstanceOf[Double]))
    case _ => println("_")
  }*/
//  def test(a : Any) = a match {
//    case a : List[String] => println("test type erase")
//    case _ =>
//  }

}
