package edu.nju.pasalab.mt.extraction.util.dependency

import java.util.regex.{Matcher, Pattern}

/**
 * Basic data structure for representing a node in a dependency tree.
 * Provides functions for getting this node from a stanford output format.
 */
class DependencyNode extends Serializable{
  var index:Int = -1
  var parent:Int = -1
  var label:String = ""
  var word:String = ""


  def this(index: Int, parent:Int, label:String, word:String) {
    this()
    this.index = index
    this.parent = parent
    this.label = label
    this.word = word
  }
  override def toString() =
    if (label == null) "(" + index + "," + parent + ")" else "(" + index + "," + parent + ")"


}
object DependencyNode {
  //(0,1,SBJ) (1,-1,ROOT) (2,3,NMOD) (3,1,OBJ) (4,3,NMOD) (5,7,NMOD) (6,7,NMOD) (7,4,PMOD)
  val regex:String = "\\((\\d+),(-?\\d+)(,(\\S+))?\\)"
  val pattern:Pattern = Pattern.compile(regex)

  def parseStringItem(item:String): DependencyNode = {
    var dn : DependencyNode = null
    val matcher : Matcher = pattern.matcher(item)
    if (matcher.matches()) {
      dn = new DependencyNode
      dn.index = matcher.group(1).toInt
      dn.parent = matcher.group(2).toInt
      if (matcher.groupCount() > 2)
        dn.label = matcher.group(4)
    }
    dn
  }
}

