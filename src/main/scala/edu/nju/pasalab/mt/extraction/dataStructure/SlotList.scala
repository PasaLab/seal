package edu.nju.pasalab.mt.extraction.dataStructure

import java.util.ArrayList
import java.util.Collections

class SlotList {
  var sList = new java.util.ArrayList[Slot]()

  def this(s: SlotList) {
    this()
    val len = s.sList.size()
    for (i <- 0 until len) {
      val h = s.sList.get(i)
      sList.add(h)
    }
  }

  def Add(slot: Slot) {
    sList.add(slot)
  }

  def IsOverlapWith(slot: Slot): Boolean = {
    val len = sList.size()
    for (i <- 0 until len) {
      val h = sList.get(i)
      if (Slot.isOverlap(h, slot, false)) return true
    }
    false
  }

  def IsNeighborWith(slot: Slot): Boolean = {
    val len = sList.size()
    for (i <- 0 until len) {
      val h = sList.get(i)
      if (Slot.isNeighbor(h, slot, false)) return true
    }
    false
  }

  def getCount: Int = sList.size

  def Pop = sList.remove(sList.size - 1)


  def GetSortedListByE: SlotList = {
    val sortedHList: SlotList = new SlotList(this)
    Collections.sort(sortedHList.sList)
    sortedHList
  }

  def clear = sList.clear
}
