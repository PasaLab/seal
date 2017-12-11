package edu.nju.pasalab.mt.extraction.util.alignment

class AlignedSentence (val srcSentence: Sentence, val trgSentence: Sentence) {

  var alignment: AlignmentTable = new AlignmentTable(srcSentence.sentSize,trgSentence.sentSize)

  def this(src: String, trg: String, align: String) {
    this(new Sentence(src), new Sentence(trg))
    alignment.fillMatrix_aligntable(align)
  }

}
