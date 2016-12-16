package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.index.{DocValues, IndexReader}
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.Logging

/**
 * @author debasish83 on 12/15/16.
 */
class LuceneShard(reader: IndexReader) extends IndexSearcher(reader) with Logging {
  logInfo(s"lucene shard leaf readers ${leafContexts.size}")
  val visits = DocValues.getNumeric(leafContexts.get(0).reader(), "visits")
}

object LuceneShard {
  def apply(reader: IndexReader): LuceneShard = {
    new LuceneShard(reader)
  }
}
