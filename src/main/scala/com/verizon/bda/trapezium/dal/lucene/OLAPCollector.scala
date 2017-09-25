package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Collector, LeafCollector, Scorer}
import java.util.BitSet

/**
  * @author debasish83 document id collector for OLAP aggregations
  *         Currently we are using lucene doc values but parquet values can be
  *         used as long as there is a unique docID consistent with parquet
  */
class OLAPCollector(docs: BitSet) extends Collector with LeafCollector {
  //TODO: use docBase from OLAPCollector to cleanup DocValueLocator clumsiness
  var docBase: Int = _

  override def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    docBase = context.docBase
    this
  }

  override def setScorer(scorer: Scorer): Unit = {
    // no-op by default
  }

  override def collect(doc: Int): Unit = {
    docs.set(docBase + doc)
  }

  override def needsScores(): Boolean = {
    return false
  }
}
