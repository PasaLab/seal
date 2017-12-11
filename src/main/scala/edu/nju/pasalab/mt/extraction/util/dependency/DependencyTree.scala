package edu.nju.pasalab.mt.extraction.util.dependency

import java.util.ArrayList
import java.util.HashSet


/**
 * Data structure for a dependency tree.Provides functions for reading and writing the tree from and to files,
 * deciding the shape and structure of the tree (e.g.whether it is well formed).
 */
class DependencyTree extends Serializable {

  var words:Array[String] = null
  var parent:Array[Int] = null
  var root:Int = -1
  var label:Array[String] = null

  //val emph:Array[Boolean] = null


  def addDenpency (dn: DependencyNode) : Unit = {
    if (dn == null)
      return
    if (dn.label != null) {
      if (label == null)
        label = new Array[String](words.length)
      label(dn.index) = dn.label
    }
    parent(dn.index) = dn.parent
    if (dn.word != null)
      words(dn.index) = dn.word
    if (dn.parent == -1)
      root = dn.index
  }

  def this(words:Array[String], parent:Array[Int], root:Int, label:Array[String]) {
    this()
    this.words = words
    this.parent = parent
    this.root = root
    this.label = label
  }
  def this(relations:String, sentence:String) {
    this()

    relations.split(" ").map(elem => DependencyNode.parseStringItem(elem)).foreach(elem => addDenpency(elem))
      //.map(elem => addDenpency(elem))
    this.words = sentence.split(" ")
  }
  def this(relations:String, words:Array[String]){
   this()

    this.words = words
    relations.split(" ").map(elem => DependencyNode.parseStringItem(elem)).foreach(elem => addDenpency(elem))
    parent = new Array[Int](words.length)
  }
  def this(words:Array[String],parent:Array[Int]){
    this()
    this.parent = parent
    var i = 0
    var judge:Boolean = false
    while (i < parent.length && !judge) {
      if (parent(i) == -1) {
        root = i
        judge = true
      }
      i += 1
    }
  }
  def this(words:Array[String]) {
    this()
    this.words = words
    parent = new Array[Int](words.length)
    label = new Array[String](words.length)
  }
  def this(length:Int) {
    this()
    this.words =new Array[String](length)
    parent = new Array[Int](length)
    label = new Array[String](length)
  }
  def this(t: DependencyTree) {
    this()
    this.root = t.root
    this.words = new Array[String](t.words.length)
    this.parent = new Array[Int](t.parent.length)
    for(i <- t.parent.indices) {
      this.words(i) =  new String(t.words(i))
      this.parent(i) = t.parent(i)
    }
    if (t.label != null){
      this.label = new Array[String](t.label.length)
      for (i <- t.label.indices)
        this.label(i) = new String(t.label(i))
    }
  }
  def readHeadPos(str:String) {
    val heads = str.split(" ")
    if (heads.length != words.length) {
      println("Possibly mismatching dependency string: " + str )
      return
    }
    parent = heads.map(_.toInt)
  }

  /**
   *words,parents,root label
   * one possible problem here: the removed node could be the root
   * in this case, the first child of root is selected as new root
   * @param index index
   * @return no
   */
  def removeNode(index:Int):DependencyTree = {
    val newWords = new Array[String](words.length - 1)
    val newParents = new Array[Int](parent.length - 1)
    val newLabel = new Array[String](label.length - 1)
    var currentParent = parent(index)
    if(currentParent > index)
      currentParent -= 1
    val rootRemoved = false
    var firstChild = true
    var newRoot = root
    for (i <- 0 until words.length - 1) {
      if (i < index) {
        newWords(i) = words(i)
        newParents(i) = parent(i)
        newLabel(i) = label(i)
      }
      else if(i >= index) {
        newWords(i) = words(i+1)
        newParents(i) = parent(i+1)
        newLabel(i) = label(i+1)
      }
      if (newParents(i) > index)
        newParents(i) -= 1
      else if(newParents(i) == index) {
        if (rootRemoved) {
          if (firstChild){
            newParents(i) -= 1
            firstChild = false
            newRoot = i
          } else
            newParents(i) = newRoot
        } else
          newParents(i) = currentParent
      }
    }
    new DependencyTree(newWords, newParents, newRoot, newLabel)
  }
  override  def toString(): String = {
    val sb = new StringBuilder(32)
    var first = true
    for (i <- words.indices) {
      if(first)
        first = false
      else
        sb.append(" ")
      if(label != null)
        sb.append(sb.append(new DependencyNode(i, parent(i), label(i), words(i)).toString()))
      else
        sb.append(sb.append(new DependencyNode(i, parent(i), null, words(i)).toString()))
    }
    sb.toString()
  }
  def toIndexString() : String = {
    val sb = new StringBuilder(32)
    var first = true
    for (i <- words.indices) {
      if(first)
        first = false
      else
        sb.append(" ")
     sb.append(parent(i))
    }
    sb.toString()
  }
  def getChildren(index:Int):Array[Int] = {
    val list:java.util.ArrayList[Int] = new java.util.ArrayList[Int]
    for (i <- words.indices; if i < parent.length) {
      if(parent(i) == index)
        list.add(i)
    }
    val childern = new Array[Int](list.size())
    for(i <- 0 until list.size())
      childern(i) = list.get(i)
    childern
  }
  def IsWellFormed(beg:Int, end:Int, relaxed:Boolean):Boolean = {
    IsFixed(beg, end, relaxed) != Int.MinValue || IsFloating(beg, end, relaxed) != Int.MinValue
  }
  def IsFixed(beg:Int, end:Int, head:Int):Boolean = {
    head != Int.MinValue
  }
  /**
   * [beg , end)
   * [beg, end) is fixed on head h (h in [beg, end) ), has three conditions:
   * 1. head of h is outside of [beg, end)       //h is the head of the structure
   * 2. for any k in [beg, end) and k!=h, head of k is in [beg, end) // item other than h, only depend on inside items
   * 3. for any k outside of [beg, end) , head of k is h, or outside of [beg, end)  // outside item only depends on h
   */
  def IsFixed(beg:Int, end:Int, relaxed:Boolean):Int = {
    //var result:Int = 0
    //condition 1&2
    val heads:ArrayList[Int] = new ArrayList[Int]()
    for (i <- beg until end; if i < heads.size())
      if(parent(i) < beg || parent(i) >= end)
        heads.add(i)
    if (heads.size() != 1)
      return Int.MinValue
    val head = heads.get(0)
    //condition 3
    if (!relaxed){
      for(i <- 0 until beg; if i < parent.size)
        if(parent(i) != head && parent(i) >= beg && parent(i) < end)
          return Int.MinValue
      for(i <- end until parent.length; if i < parent.size)
        if(parent(i) != head && parent(i) >= beg && parent(i) < end)
          return Int.MinValue
    }
    head
  }

  /**
   * [beg, end)
   * [beg, end) is a floating structure with children C, has three conditions:
   * 1. all head of C is the same
   * 2. for any k in [beg, end) and k not in C, head of k is in [beg, end) // item not in C, only depend on inside items
   * 3. for any k outside of [beg, end) , head of k is outside of [beg, end)  // outside item does not depend on inside items
   */
  def IsFloating(beg:Int, end:Int, relaxed:Boolean): Int = {
    val heads:HashSet[Int] = new HashSet
    for(i <- beg until end; if i < parent.length)
      if(parent(i) < beg || parent(i) >= end)
        heads.add(parent(i))
    //condition 1
    if(heads.size() != 1)
      return Int.MinValue
    val Lhead:ArrayList[Int] = new ArrayList[Int](heads)
    val head = Lhead.get(0)

    //condition 2
    for (i <- beg until end; if i < parent.length)
      if (parent(i) != head && parent(i) < beg && parent(i) >= end)
        return Int.MinValue
    if (!relaxed){
      //condition 3
      for(i <- 0 until beg; if i < parent.length)
        if(parent(i) >=beg && parent(i) < end)
          return Int.MinValue
      for (i <- end until parent.length; if i < parent.length)
        if(parent(i) >= beg && parent(i) < end)
          return Int.MinValue
    }
    head
  }
  def IsLeftFloating(beg:Int, end:Int, head:Int) =
    if(head != Int.MinValue && head >= beg) true else false

  def IsRigthFloating(beg:Int, end:Int, head:Int) =
    if (head != Int.MinValue && head < beg) true else false

  def getSubtree(beg:Int, end:Int): Array[Int] ={
    val subtree:Array[Int] = new Array[Int](end - beg)
    for (i <- beg until end; if i < parent.length && i < subtree.length) {
      val head = parent(i)
      if (head >= beg && head < end)
        subtree(i - beg) = head - beg
      else
        subtree(i - beg) = -1
    }
    subtree
  }
}
object DependencyTree {
  import scala.io.Source
  import java.io._
  def dep2head(): Unit = {
    val r = Source.fromFile("********************").mkString.split("\\s+").iterator
    val pw = new PrintStream("outDir")
    while (r.hasNext) {
      val s = r.next()
      if (s.equals(""))
        pw.println()
      else {
        val content = s.split("\\) \\(")
        pw.println(new DependencyTree(s, content).toIndexString())
      }
    }
    pw.close()
  }
  def head2dep(): Unit ={
    val r = Source.fromFile("********************").mkString.split("\\s+").iterator
    val pw = new PrintStream("outDir")
    while (r.hasNext) {
      val str = r.next()
      if(str.equals(""))
        pw.println()
      else {
        val index = str.split(" ")
        for (i <- 0 until index.length)
          pw.print("(" + i + "," + index + ")")
        pw.println()
      }
    }
    pw.close()
  }
  def main(args:Array[String]): Unit = {
    head2dep()
  }
}
object Format extends Enumeration {
  type Format = Value
  val dep, stanford, mst = Value
}