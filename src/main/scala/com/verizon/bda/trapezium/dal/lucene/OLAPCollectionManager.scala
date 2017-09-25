package com.verizon.bda.trapezium.dal.lucene

import java.util.Collection
import org.apache.lucene.search.CollectorManager
import org.apache.spark.Logging
import java.util.BitSet

/**
  * @author debasish83 collection manager for documents that matched the OLAP query
  */
class OLAPCollectionManager(maxDocs: Int) extends CollectorManager[OLAPCollector, BitSet] with Logging {
  log.info(s"max docs within partition ${maxDocs}")

  // Generate a BitSet and add the docs that matched search filter, if needed back by RoaringBitMap
  val docs = new BitSet(maxDocs)

  override def newCollector(): OLAPCollector = {
    return new OLAPCollector(docs)
  }

  override def reduce(collectors: Collection[OLAPCollector]): BitSet = {
    docs
  }
}

