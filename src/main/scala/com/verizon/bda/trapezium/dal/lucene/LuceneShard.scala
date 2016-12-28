package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{ScoreDoc, BooleanQuery, IndexSearcher}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.TimestampType

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

  // Filter the time column for time series analysis
  val timeColumns = converter.types.keys.filter { case column =>
    converter.types(column).dataType == TimestampType
  }.toSeq

  assert(timeColumns.size <= 1, s"more than one timestamp columns not supported")

  val timeColumn =
    if (timeColumns.size >= 1) Option(timeColumns.head)
    else None

  def searchDocs(queryStr: String, sample: Double = 1.0): Array[ScoreDoc] = {
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt
    val topDocs = search(qp.parse(queryStr), maxRowsPerPartition)
    log.info("Hits within partition: " + topDocs.totalHits)
    topDocs.scoreDocs
  }

  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row] = {
    val scoredDocs = searchDocs(queryStr, sample)
    // TODO: Scoring of docs is not required for queries that are not extracting relevance
    val rows = scoredDocs.iterator.map { d =>
      // Extract stored fields
      val stored = converter.docToRow(doc(d.doc))
      val docValued = dvExtractor.extract(columns, d.doc)
      Row.merge(stored, docValued)
    }
    rows
  }

  // If timestamp < minTime skip
  // If timestamp >= maxTime skip
  def filterTime(scored: Array[ScoreDoc],
                 minTime: Long,
                 maxTime:Long): Array[ScoreDoc] = {
    scored.filter { case (doc) =>
      val docID = doc.doc
      val offset = dvExtractor.getOffset(timeColumn.get, docID)
      var filter = true
      var i = 0
      while (i < offset) {
        val timestamp = dvExtractor.extract(timeColumn.get, docID, i).asInstanceOf[Long]
        if (timestamp < minTime) filter = false
        if (timestamp >= maxTime) filter = false
        i += 1
      }
      filter
    }
  }

  //TODO: Push time filters on group
  def group(queryStr: String,
            dimension: String,
            dimOffset: Int,
            measure: String,
            minTime: Long = 0L,
            maxTime: Long = Long.MaxValue,
            agg: LuceneAggregator): LuceneAggregator = {
    val scoredDocs = searchDocs(queryStr)

    val filteredDocs = if (timeColumn == None) {
      log.warn(s"no timestamp in dataset for time [$minTime, $maxTime] filter")
      scoredDocs
    } else {
      filterTime(scoredDocs, minTime, maxTime)
    }

    var i = 0
    while (i < filteredDocs.size) {
      val docID = filteredDocs(i).doc
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

  // Given a timestamp, find timestamp - minTime % rollup to find the index
  // statistics of the timestamp column should give minTime and maxTime
  // user specified min and max time overwrite the column stats
  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String,
                 agg: LuceneAggregator): LuceneAggregator = {
    assert(timeColumn == None, s"no timestamp in dataset for time series aagregation")
    val scoredDocs = searchDocs(queryStr)

    val filteredDocs = filterTime(scoredDocs, minTime, maxTime)

    var i = 0
    while(i < filteredDocs.size) {
      val docID = filteredDocs(i).doc
      val docMeasure = dvExtractor.extract(measure, docID, 0)
      val offset = dvExtractor.getOffset(timeColumn.get, docID)
      var j = 0
      while (j < offset) {
        val timestamp = dvExtractor.extract(timeColumn.get, docID, j).asInstanceOf[Long]
        val idx = (timestamp - minTime) % rollup
        agg.update(idx.toInt, docMeasure)
        j += 1
      }
      i += 1
    }
    agg
  }
}

object LuceneShard {
  def apply(reader: IndexReader,
            converter: OLAPConverter): LuceneShard = {
    new LuceneShard(reader, converter)
  }
}
