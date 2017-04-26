package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanQuery, IndexSearcher, ScoreDoc}
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

  // TODO: LeafReader > 1 are shards created by singlethreaded index writer due to flush limits
  // TODO: Each flush limit generates a new shard

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

  def searchDocs(queryStr: String,
                 sample: Double = 1.0): Array[Int] = {
    // TODO: Provide sampling tricks that give provable count stats without affecting accuracy
    // val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt

    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val query = rewrite(qp.parse(queryStr))
    val collectionManager = new OLAPCollectionManager(reader.maxDoc())
    // TODO: this.executorService to run searches in multiple cores while partitions are done
    // at executor level
    search(query, collectionManager)
  }

  def searchDocsWithRelevance(queryStr: String, sample: Double = 1.0): Array[ScoreDoc] = {
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt
    val topDocs = search(qp.parse(queryStr), maxRowsPerPartition)
    log.info("Hits within partition: " + topDocs.totalHits)
    topDocs.scoreDocs
  }

  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row] = {
    val docs = searchDocs(queryStr, sample)
    // TODO: Scoring of docs is not required for queries that are not extracting relevance
    val rows = docs.iterator.map { d =>
      // Extract stored fields
      val stored = converter.docToRow(doc(d))
      reader.document(d)
      val docValued = dvExtractor.extract(columns, d)
      Row.merge(stored, docValued)
    }
    rows
  }

  // If timestamp < minTime skip, timestamp >= maxTime skip
  // TODO: For numeric fields, they can be passed through such filters as well, look into
  // integrating time filter as dataframe filter
  def filterTime(docs: Array[Int],
                 minTime: Long,
                 maxTime: Long): Array[Int] = {
    docs.filter { case (docID) =>
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

  // TODO: Multiple measures can be aggregated
  // TODO: Generalize it to dataframe aggregate
  def aggregate(queryStr: String,
                measure: String,
                agg: OLAPAggregator): OLAPAggregator = {
    val scoreStart = System.nanoTime()
    val docs = searchDocs(queryStr)
    log.info(f"document scoring time: ${(System.nanoTime() - scoreStart) * 1e-9}%6.3f sec")

    val aggStart = System.nanoTime()
    var i = 0
    while (i < docs.size) {
      val docID = docs(i)
      val docMeasure = dvExtractor.extract(measure, docID, 0)
      agg.update(0, docMeasure)
      i += 1
    }
    log.info(f"document aggregation time :${(System.nanoTime() - aggStart) * 1e-9}%6.3f sec")
    agg
  }

  // TODO: Generalize it to Dataframe groupBy
  def group(queryStr: String,
            dimension: String,
            dimOffset: Int,
            measure: String,
            minTime: Long = 0L,
            maxTime: Long = Long.MaxValue,
            agg: OLAPAggregator): OLAPAggregator = {

    val scoreStart = System.nanoTime()
    val docs = searchDocs(queryStr)
    log.info(f"document scoring time: ${(System.nanoTime() - scoreStart) * 1e-9}%6.3f sec")

    val filterStart = System.nanoTime()
    val filteredDocs = if (timeColumn == None) {
      log.warn(s"no timestamp in dataset for time [$minTime, $maxTime] filter")
      docs
    } else {
      filterTime(docs, minTime, maxTime)
    }
    log.info(f"document filtering time :${(System.nanoTime() - filterStart) * 1e-9}%6.3f sec")

    log.info(s"scored docs ${docs.length} filtered docs ${filteredDocs.length}")

    val aggStart = System.nanoTime()
    var i = 0
    while (i < filteredDocs.size) {
      val docID = filteredDocs(i)
      // TODO: When measure is sparse, then dvExtractor extract with offset should be called
      val docMeasure = dvExtractor.extract(measure, docID, 0)
      val offset = dvExtractor.getOffset(dimension, docID)
      var j = 0
      while (j < offset) {
        val idx = dvExtractor.extract(dimension, docID, j).asInstanceOf[Long].toInt
        val scaledIdx = idx - dimOffset
        // Either docMeasure will be called measureOffset times
        // OR docMeasure will be called with scaledIdx offset
        // We take a sparse flag ? does it generalize ?
        // TODO: If the document does not have value, 0 is returned, for age field with ? 0
        // TODO: docvalue is being returned, why ? clean the if condition
        if (scaledIdx >= 0) agg.update(scaledIdx, docMeasure)
        j += 1
      }
      i += 1
    }
    log.info(f"document aggregation time :${(System.nanoTime() - aggStart) * 1e-9}%6.3f sec")
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
                 agg: OLAPAggregator): OLAPAggregator = {
    assert(timeColumn != None, s"no timestamp in dataset for time series aggregation")
    val scoreStart = System.nanoTime()
    val docs = searchDocs(queryStr)
    log.info(f"document scoring time: ${(System.nanoTime() - scoreStart) * 1e-9}%6.3f sec")

    val filterStart = System.nanoTime()
    val filteredDocs = filterTime(docs, minTime, maxTime)
    log.info(f"document filtering time :${(System.nanoTime() - filterStart) * 1e-9}%6.3f sec")

    log.info(s"scored docs ${docs.length} filtered docs ${filteredDocs.length}")

    val aggStart = System.nanoTime()
    var i = 0
    while (i < filteredDocs.size) {
      val docID = filteredDocs(i)
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
    log.info(f"document aggregation time :${(System.nanoTime() - aggStart) * 1e-9}%6.3f sec")
    agg
  }
}

object LuceneShard {
  def apply(reader: IndexReader,
            converter: OLAPConverter): LuceneShard = {
    new LuceneShard(reader, converter)
  }
}
