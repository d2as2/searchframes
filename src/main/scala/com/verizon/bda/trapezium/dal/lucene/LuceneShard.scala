package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanQuery, IndexSearcher}
import org.apache.spark.Logging
import org.apache.spark.sql.Row

/**
 * @author debasish83 on 12/15/16.
 *         Supports primitives for searching and aggregation on one(topk)/multiple(groupby) dimension given set
 *         of measures
 */

class LuceneShard(reader: IndexReader,
                  converter: OLAPConverter) extends IndexSearcher(reader) with Logging {
  logInfo(s"lucene shard leaf readers ${leafContexts.size}")

  //TODO: LeafReader > 1 need further understanding
  assert(leafContexts.size() <= 1, s"${leafContexts.size()} leafReaders per spark partition")

  val leafReader =
    if (leafContexts.size() > 0) leafContexts.get(0).reader()
    else null

  val dvExtractor = DocValueExtractor(leafReader, converter)
  val analyzer = new KeywordAnalyzer

  val qp = new QueryParser("content", analyzer)

  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row] = {
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt
    val topDocs = search(qp.parse(queryStr), maxRowsPerPartition)

    // TODO: Scoring of docs is not required for queries that are not extracting relevance
    val rows = topDocs.scoreDocs.iterator.map { d =>
      // Extract stored fields
      val stored = converter.docToRow(doc(d.doc))
      val docValued = dvExtractor.extract(columns, d.doc)
      Row.merge(stored, docValued)
    }
    log.debug("Hits within partition: " + topDocs.totalHits)
    rows
  }

  //TODO: Add time filters on group
  def group(queryStr: String,
            dimension: String,
            dimOffset: Int,
            measure: String,
            agg: LuceneAggregator): LuceneAggregator = {
    val sample = 1.0
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt
    val topDocs = search(qp.parse(queryStr), maxRowsPerPartition).scoreDocs

    // TODO: If extract gives me back the data type we are ok defining the aggregation
    // TODO: In the internal while loops there should be any case class
    var i = 0
    while (i < topDocs.size) {
      val docID = topDocs(i).doc
      val docMeasure = dvExtractor.extract(measure, docID, 0)

      val offset = dvExtractor.getOffset(dimension, docID)
      var j = 0
      while (j < offset) {
        val idx = dvExtractor.extract(dimension, docID, j).asInstanceOf[Long].toInt
        agg.update(idx - dimOffset, docMeasure)
        j += 1
      }
      i += 1
    }
    agg
  }

  //TODO: Do we need grouping dimension here as well ?
  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String): Iterator[Row] = {
    ???
  }
}

object LuceneShard {
  def apply(reader: IndexReader,
            converter: OLAPConverter): LuceneShard = {
    new LuceneShard(reader, converter)
  }
}
