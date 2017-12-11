package edu.nju.pasalab.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import gnu.trove.map.hash.TObjectDoubleHashMap

/**
  * Created by Cloud on 2016-01-25.
  */
class TObjectDoubleHashMapSerializer extends Serializer[TObjectDoubleHashMap[String]]{
  override def write(kryo: Kryo,
                     output: Output,
                     map: TObjectDoubleHashMap[String]): Unit = {
    output.writeInt(map.size())
    var count = 0
    val it = map.iterator()
    while (it.hasNext){
      it.advance()
      output.writeString(it.key())
      output.writeDouble(it.value())
      count += 1
    }
    assert(count == map.size())
  }

  override def read(kryo: Kryo,
                    input: Input,
                    _type: Class[TObjectDoubleHashMap[String]]): TObjectDoubleHashMap[String] = {
    val size = input.readInt()
    val map = new TObjectDoubleHashMap[String](size)
    var count = 0
    while(count < size){
      val key = input.readString()
      val value = input.readDouble()
      map.put(key, value)
      count += 1
    }
    map
  }
}
