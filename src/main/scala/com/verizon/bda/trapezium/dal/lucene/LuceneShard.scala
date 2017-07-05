package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{BooleanQuery, IndexSearcher, Query, ScoreDoc}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.TimestampType
import java.util.BitSet
import scala.collection.mutable.ArrayBuffer

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
  // time column should be a measure field
  val timeColumns = converter.measures.seq.filter { case column =>
    converter.schema(column).dataType == TimestampType
  }.toSeq

  assert(timeColumns.size <= 1, s"more than one timestamp columns not supported")

  val timeColumn =
    if (timeColumns.size >= 1) {
      log.info(s"timestamp column ${timeColumns.head} selected in dataset")
      Option(timeColumns.head)
    }
    else None

  def searchDocs(queryStr: String,
                 sample: Double = 1.0): BitSet = {
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
    val maxRowsPerPartition = Math.floor(sample * getIndexReader.numDocs()).toInt
    val topDocs = search(qp.parse(queryStr), maxRowsPerPartition)
    log.info("Hits within partition: " + topDocs.totalHits)
    topDocs.scoreDocs
  }

  /* TODO: Generate a document iterator over BitSet
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val d = docs.nextSetBit(i)
      Transform(d)
      i = docs.nextSetBit(i + 1)
    }
  */
  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row] = {
    val docs = searchDocs(queryStr, sample)
    // TODO: Scoring of docs is not required for queries that are not extracting relevance
    val rowBuilder = new ArrayBuffer[Row](docs.cardinality())

    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val d = docs.nextSetBit(i)
      // Extract stored fields
      val stored = converter.docToRow(doc(d))
      reader.document(d)
      val docValued = dvExtractor.extract(columns, d)
      rowBuilder += Row.merge(stored, docValued)
      i = docs.nextSetBit(i + 1)
    }
    rowBuilder.result().iterator
  }

  // If timestamp < minTime skip, timestamp >= maxTime skip
  // TODO: For numeric fields, they can be passed through such filters as well, look into
  // integrating time filter as dataframe filter
  def filter(docs: BitSet,
             column: String,
             min: Long,
             max: Long): BitSet = {
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val docID = docs.nextSetBit(i)
      // TODO: time column can be multi-value
      val timestamp = dvExtractor.extract(column, docID, 0).asInstanceOf[Long]
      if (timestamp < min) docs.flip(docID)
      if (timestamp >= max) docs.flip(docID)
      i = docs.nextSetBit(i + 1)
    }
    docs
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
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val docID = docs.nextSetBit(i)
      val measureOffset = dvExtractor.getOffset(measure, docID)
      if (measureOffset > 0) {
        val docMeasure = dvExtractor.extract(measure, docID, 0)
        agg.update(0, docMeasure)
      }
      i = docs.nextSetBit(i + 1)
    }
    log.info(f"document aggregation time :${(System.nanoTime() - aggStart) * 1e-9}%6.3f sec")
    agg
  }

  //TODO: Use term dictionary to extract dimension and avoid dv seeks
  def facet(queryStr: String,
            dimension: String,
            dimOffset: Int,
            agg: OLAPAggregator): OLAPAggregator = {
    val scoreStart = System.nanoTime()
    val docs = searchDocs(queryStr)
    log.info(f"document scoring time: ${(System.nanoTime() - scoreStart) * 1e-9}%6.3f sec")

    val aggStart = System.nanoTime()
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val docID = docs.nextSetBit(i)
      val offset = dvExtractor.getOffset(dimension, docID)
      var j = 0
      while (j < offset) {
        val idx = dvExtractor.extract(dimension, docID, j).asInstanceOf[Long].toInt
        val scaledIdx = idx - dimOffset
        agg.update(scaledIdx, 1L)
        j += 1
      }
      i = docs.nextSetBit(i + 1)
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
      filter(docs, timeColumn.get, minTime, maxTime)
    }

    log.info(f"document filtering time :${(System.nanoTime() - filterStart) * 1e-9}%6.3f sec")

    log.info(s"scored docs ${docs.cardinality()} filtered docs ${filteredDocs.cardinality()}")

    val aggStart = System.nanoTime()
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val docID = docs.nextSetBit(i)
      val measureOffset = dvExtractor.getOffset(measure, docID)
      if (measureOffset > 0) {
        val docMeasure = dvExtractor.extract(measure, docID, 0)
        val offset = dvExtractor.getOffset(dimension, docID)
        var j = 0
        while (j < offset) {
          val idx = dvExtractor.extract(dimension, docID, j).asInstanceOf[Long].toInt
          val scaledIdx = idx - dimOffset
          agg.update(scaledIdx, docMeasure)
          j += 1
        }
      }
      i = docs.nextSetBit(i + 1)
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
    val filteredDocs = filter(docs, timeColumn.get, minTime, maxTime)
    log.info(f"document filtering time :${(System.nanoTime() - filterStart) * 1e-9}%6.3f sec")

    log.info(s"scored docs ${docs.cardinality()} filtered docs ${filteredDocs.cardinality()}")

    val aggStart = System.nanoTime()
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val docID = docs.nextSetBit(i)
      val measureOffset = dvExtractor.getOffset(measure, docID)
      if (measureOffset > 0) {
        val docMeasure = dvExtractor.extract(measure, docID, 0)
        val offset = dvExtractor.getOffset(timeColumn.get, docID)
        var j = 0
        while (j < offset) {
          val timestamp = dvExtractor.extract(timeColumn.get, docID, j).asInstanceOf[Long]
          val idx = (timestamp - minTime) / rollup
          agg.update(idx.toInt, docMeasure)
          j += 1
        }
      }
      i = docs.nextSetBit(i + 1)
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
