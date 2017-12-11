package edu.nju.pasalab.mt.extraction.dataStructure


object SpecialString {

  val mainSeparator:String = " ||| "
  val fileSeparator:String = "\t"
  val nullWordString:String = "<NULL>"
  val nullAlignString:String= "0-0"
  val secondarySeparator = " |||| "
  val listItemSeparator = " "
  /**
   *mainSeparator = " ||| ";
   *mainSeparatorForSplit = " \\|\\|\\| ";
   *secondarySeparator = " |||| ";
   *secondarySeparatorForSplit = " \\|\\|\\|\\| ";
   *thirdSeparator = "\t";
   *listItemSeparator = " ";
   *hadoopFileSeparator = "\t";
  */
}

object Splitter{
  val mainSplitter = " \\|\\|\\| "
}
