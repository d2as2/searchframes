package com.verizon.bda.trapezium.dal.lucene

import java.util.Collection

import org.apache.lucene.search.CollectorManager
import org.apache.spark.Logging

/**
  * @author debasish83 collection manager for documents that matched the OLAP query
  */
class OLAPCollectionManager(maxDoc: Int) extends CollectorManager[OLAPCollector, Array[Int]] with Logging {
  override def newCollector(): OLAPCollector = {
    return new OLAPCollector(maxDoc)
  }

  override def reduce(collectors: Collection[OLAPCollector]): Array[Int] = {
    var matchedDocs = 0
    val collectionIter = collectors.iterator()
    while (collectionIter.hasNext) {
      matchedDocs += collectionIter.next().getSize()
    }
    val matchedDocIds = Array.fill[Int](matchedDocs)(-1)
    log.info(s"Hits within partition: ${matchedDocs} collectors: ${collectors.size()}")

    val collectionDocIter = collectors.iterator()
    var pos = 0
    while (collectionDocIter.hasNext) {
      val collector = collectionDocIter.next()
      val maxDocs = collector.getSize()
      val docs = collector.getDocs()
      log.info(s"collector $pos docs ${maxDocs}")
      System.arraycopy(docs, 0, matchedDocIds, pos, maxDocs)
      pos += maxDocs
    }
    matchedDocIds
  }
}
