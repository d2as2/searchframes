package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.BooleanQuery
import org.apache.spark.sql.Row
import org.apache.spark.{TaskContext, SparkContext, Partition}
import org.apache.spark.rdd.RDD
/**
 * @author debasish83 on 12/15/16.
 */
class LuceneRDD(sc: SparkContext, location: String) extends RDD[LuceneShard](sc, Nil) {
  override def getPartitions(): Array[Partition] = {
    ???
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[LuceneShard] = {
    ???
  }

  lazy val qp = new QueryParser("content", analyzer)

  def search(queryStr: String, sample: Double = 1.0): RDD[Row] = {
    val rows = this.flatMap((shard: LuceneShard) => {
      BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
      val maxRowsPerPartition = Math.floor(sample * shard.getIndexReader.numDocs()).toInt
      val topDocs = shard.search(qp.parse(queryStr), maxRowsPerPartition)

      log.debug("Hits within partition: " + topDocs.totalHits)
      topDocs.scoreDocs.map { d => converter.docToRow(shard.doc(d.doc)) }
    })
    rows
  }

  def count(queryStr: String): Int = {
    this.map((shard: LuceneShard) => {
      shard.count(qp.parse(queryStr))
    }).sum().toInt
  }
}
