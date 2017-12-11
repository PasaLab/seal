package edu.nju.pasalab.mt.extraction.dataStructure

/**
 * Created by dong on 2015/3/4.
 * new Slot(startE, endE, startC, endC, 0, isWellFormedSpan, spanType, head, headPOS)
 */
class Slot(val startE:Int,val endE:Int,val startC:Int,val endC:Int,val index:Int, val isWellFormSpan:Int, val slotType:String, val head:Int,val headPOS:String) extends Comparable[Slot]{


  def compareTo (o: Slot):Int = startE - o.startE

  def getLengthC: Int = endC - startC + 1

  def getLengthE: Int = endE - startE + 1

}

object Slot{
  def isOverlap(sA: Slot, sB: Slot, isSrc: Boolean): Boolean = {
    if (isSrc) return !(sA.startC > sB.endC || sA.endC < sB.startC)
    !(sA.startE > sB.endE || sA.endE < sB.startE)
  }

  def isNeighbor(sA: Slot, sB: Slot, isSrc: Boolean): Boolean = {
    if (isSrc) return sA.startC == sB.endC + 1 || sA.endC == sB.startC - 1
    sA.startE == sB.endE + 1 || sA.endE == sB.startE - 1
  }
}