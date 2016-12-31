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

case class LuceneReader(leafReader: LeafReader, range: FeatureAttr)

class LuceneShard(reader: IndexReader,
                  converter: OLAPConverter) extends IndexSearcher(reader) with Logging {
  logInfo(s"lucene shard leaf readers ${leafContexts.size}")

  //TODO: LeafReader > 1 are shards created by singlethreaded index writer due to flush limits
  //TODO: Each flush limit generates a new shard

  /* On one specific node

  Better is to have range since they might not be ordered

  0 -> [0, 327680]
  1 -> [327680, 327656]
  2 -> [655336, 32768]
  3 -> [688104, 26642]

  These range can be re-ordered as well based on the ordering of leafContext

  0 -> [0, 327680]
  1 -> [327680, 327656]
  3 -> [688104, 26642]
  2 -> [655336, 32768]

  docID can fall in one of the range, return that index

  lucene.LuceneShard: leafContext ord 0 docBase 0 numDocs 327680
  lucene.LuceneShard: leafContext ord 1 docBase 327680 numDocs 327656
  lucene.LuceneShard: leafContext ord 2 docBase 655336 numDocs 32768
  lucene.LuceneShard: leafContext ord 3 docBase 688104 numDocs 26642
  */
  val leafReaders = (0 until leafContexts.size()).map((ctxId) => {
    val ctx = leafContexts.get(ctxId)
    val reader = ctx.reader()
    log.info(s"leafContext ord ${ctx.ord} docBase ${ctx.docBase} numDocs ${reader.numDocs()}")
    LuceneReader(reader, FeatureAttr(ctx.docBase, reader.numDocs()))
  })

  val dvExtractor = DocValueExtractor(leafReaders, converter)
  val analyzer = new KeywordAnalyzer
  val qp = new QueryParser("content", analyzer)

  // Filter the time column for time series analysis
  val timeColumns = converter.types.keys.filter { case column =>
    converter.types(column).dataType == TimestampType
  }.toSeq

  assert(timeColumns.size <= 1, s"more than one timestamp columns not supported")

  val timeColumn =
    if (timeColumns.size >= 1) {
      log.info(s"timestamp column ${timeColumns.head} selected in dataset")
      Option(timeColumns.head)
    }
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
      reader.document(d.doc)
      val docValued = dvExtractor.extract(columns, d.doc)
      Row.merge(stored, docValued)
    }
    rows
  }

  // If timestamp < minTime skip
  // If timestamp >= maxTime skip
  def filterTime(scored: Array[ScoreDoc],
                 minTime: Long,
                 maxTime: Long): Array[ScoreDoc] = {
    scored.filter { case (doc) =>
      val docID = doc.doc
      val offset = dvExtractor.getOffset(timeColumn.get, docID)
      var filter = true
      var i = 0
      while (i < offset) {
        val timestamp =
          dvExtractor.extract(timeColumn.get, docID, i).asInstanceOf[Long]
        if (timestamp < minTime) filter = false
        if (timestamp >= maxTime) filter = false
        i += 1
      }
      filter
    }
  }

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
    log.info(s"scored docs ${scoredDocs.length} filtered docs ${filteredDocs.length}")
    var i = 0
    while (i < filteredDocs.size) {
      val docID = filteredDocs(i).doc
      val docMeasure = dvExtractor.extract(measure, docID, 0)
      val offset = dvExtractor.getOffset(dimension, docID)
      var j = 0
      while (j < offset) {
        val idx = dvExtractor.extract(dimension, docID, j).asInstanceOf[Long].toInt
        val scaledIdx = idx - dimOffset
        //TODO: If the document does not have value, 0 is returned, for age field with ? 0
        //TODO: docvalue is being returned, why ? clean the if condition
        if (scaledIdx >= 0) agg.update(scaledIdx, docMeasure)
        j += 1
      }
      i += 1
    }
    agg
  }

  // Given a timestamp, find timestamp - minTime / rollup to find the index
  // statistics of the timestamp column should give minTime and maxTime
  // user specified min and max time overwrite the column stats
  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String,
                 agg: LuceneAggregator): LuceneAggregator = {
    assert(timeColumn != None, s"no timestamp in dataset for time series aggregation")
    val scoredDocs = searchDocs(queryStr)
    val filteredDocs = filterTime(scoredDocs, minTime, maxTime)

    log.info(s"scored docs ${scoredDocs.length} filtered docs ${filteredDocs.length}")

    var i = 0
    while (i < filteredDocs.size) {
      val docID = filteredDocs(i).doc
      val docMeasure = dvExtractor.extract(measure, docID, 0)
      val offset = dvExtractor.getOffset(timeColumn.get, docID)
      var j = 0
      while (j < offset) {
        val timestamp = dvExtractor.extract(timeColumn.get, docID, j).asInstanceOf[Long]
        val idx = (timestamp - minTime) / rollup
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
