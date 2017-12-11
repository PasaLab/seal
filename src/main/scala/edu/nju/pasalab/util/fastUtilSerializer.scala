package edu.nju.pasalab.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unimi.dsi.fastutil.objects.Object2LongAVLTreeMap

/**
  * Created by wuyan on 2016/5/22.
  */
class fastUtilSerializer extends Serializer[Object2LongAVLTreeMap[String]] {
  override def write(kryo: Kryo, output: Output, map : Object2LongAVLTreeMap[String]): Unit = {

    output.writeInt(map.size())
    var count = 0
    val it = map.keySet().iterator()
    while (it.hasNext) {
      val tmp = it.next()
      output.writeString(tmp)
      output.writeLong(map.getLong(tmp))
      count += 1
    }
    assert(count == map.size())
  }

  override def read(kryo: Kryo, input: Input, _type: Class[Object2LongAVLTreeMap[String]]): Object2LongAVLTreeMap[String] = {
    val size = input.readInt()
    val map = new Object2LongAVLTreeMap[String]()
    var count = 0
    while (count < size) {
      val key = input.readString()
      val value = input.readLong()
      map.put(key, value)
      count += 1
    }
    map
  }
}
