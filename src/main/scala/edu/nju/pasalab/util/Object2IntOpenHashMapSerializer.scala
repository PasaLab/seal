package edu.nju.pasalab.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap

/**
  * Created by YWJ on 2017.1.16.
  * Copyright (c) 2017 NJU PASA Lab All rights reserved.
  */
class Object2IntOpenHashMapSerializer extends Serializer[Object2IntOpenHashMap[String]]{
  override def write(kryo: Kryo,
                     output: Output,
                     map: Object2IntOpenHashMap[String]): Unit = {
    output.writeInt(map.size())
    var count = 0
    val it = map.keySet().iterator()
    var key : String = ""
    while (it.hasNext) {
      key = it.next()
      output.writeString(key)
      output.writeDouble(map.getInt(key))
      count += 1
    }
    assert(count == map.size())
  }

  override def read(kryo: Kryo,
                    input: Input,
                    _type: Class[Object2IntOpenHashMap[String]]): Object2IntOpenHashMap[String] = {
    val size = input.readInt()
    val map = new Object2IntOpenHashMap[String](size)
    var count = 0
    while(count < size){
      val key = input.readString()
      val value = input.readInt()
      map.put(key, value)
      count += 1
    }
    map
  }
}
