package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{TopDocs, BooleanQuery, IndexSearcher}
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

  //TODO: LeafReader may need further processing
  assert(leafContexts.size() == 1, s"${leafContexts.size()} leafReaders per spark partition")
  val leafReader = leafContexts.get(0).reader()

  val dvExtractor = DocValueExtractor(leafReader, converter)
  val analyzer = new KeywordAnalyzer

  val qp = new QueryParser("content", analyzer)

  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row] = {
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
    val maxRowsPerPartition = Math.floor(sample * this.getIndexReader.numDocs()).toInt
    val topDocs: TopDocs = this.search(qp.parse(queryStr), maxRowsPerPartition)

    // TODO: Scoring of docs is not required for queries that are not extracting relevance
    val rows = topDocs.scoreDocs.iterator.map { d =>
      // Extract stored fields
      val stored = converter.docToRow(doc(d.doc))
      val docValued = dvExtractor.extract(d.doc, columns)
      Row.merge(stored, docValued)
    }
    log.debug("Hits within partition: " + topDocs.totalHits)
    rows
  }
}

object LuceneShard {
  def apply(reader: IndexReader,
            converter: OLAPConverter): LuceneShard = {
    new LuceneShard(reader, converter)
  }
}
