package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{Collector, LeafCollector, Scorer}

import scala.collection.mutable.ArrayBuffer

/**
  * @author debasish83 document id collector for OLAP aggregations
  *         Currently we are using lucene doc values but parquet values can be
  *         used as long as there is a unique docID consistent with parquet
  */
class OLAPCollector(maxDocs: Int) extends Collector with LeafCollector {
  //TODO: Based on performance better to use a pre-allocated array and counter
  private val docs = new ArrayBuffer[Int]()
  docs.sizeHint(maxDocs)

  //TODO: use docBase from OLAPCollector to cleanup DocValueLocator clumsiness
  var docBase: Int = _

  override def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    docBase = context.docBase
    this
  }

  override def setScorer(scorer: Scorer): Unit = {
    // no-op by default
  }

  def getSize(): Int = {
    docs.size
  }

  def getDocs(): Array[Int] = {
    docs.toArray
  }

  override def collect(doc: Int): Unit = {
    docs += docBase + doc
  }

  override def needsScores(): Boolean = {
    return false
  }
}
