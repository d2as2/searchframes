package com.verizon.bda.trapezium.dal.lucene

import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * @author debasish83 on 12/22/16.
 *         Supports primitives for extracting doc value fields
 */
class DocValueExtractor(leafReaders: Seq[LuceneReader],
                        converter: OLAPConverter) extends Serializable with Logging {
  val types = converter.types
  val dimensions = converter.dimensions
  val ser = converter.ser

  val leafBoundaries = leafReaders.map(_.range)

  //TODO: do a binary search if linear search is slow / datastructure to avoid if/else
  //TODO: Add a fixed size LRUCache to avoid linear scan every time
  private def readerIndex(docID: Int) : Int = {
    var i = 0
    while (i < leafBoundaries.length) {
      if (leafBoundaries(i).contains(docID)) return i
      i += 1
    }
    throw new LuceneDAOException(s"docID $docID not within leafBoundaries ${leafBoundaries.mkString(",")}")
  }

  private val dvMap: Map[String, DocValueAccessor] = if (leafReaders.length > 0) {
    types.map { case (k, v) =>
      //Dimensions have gone through DictionaryEncoding
      val dataType =
        if (dimensions.contains(k)) IntegerType
        else v.dataType
      val accessor = DocValueAccessor(leafReaders.map(_.leafReader), k, dataType, v.multiValued, ser)
      (k, accessor)
    }
  } else {
    Map.empty[String, DocValueAccessor]
  }

  //TODO: Measure can be multi-valued as well. for first iteration of time series
  //TODO: measures are considered to be single-valued
  private def extractMeasure(docID: Int, shardIndex: Int, column: String): Any = {
    assert(!dimensions.contains(column), s"$column is not a measure")
    val offset = dvMap(column).getOffset(docID, shardIndex)
    assert(offset == 1, s"measure $column is a multi-value field with offset $offset")
    dvMap(column).extract(docID - leafBoundaries(shardIndex).dictionaryPos, shardIndex, offset - 1)
  }

  private def extractDimension(docID: Int, shardIndex: Int, column: String): Any = {
    assert(dimensions.contains(column), s"$column is not a dimension")
    val offset = dvMap(column).getOffset(docID, shardIndex)
    if (offset > 1)
      Seq((0 until offset).map(dvMap(column).extract(docID, shardIndex, _)): _*)
    else dvMap(column).extract(docID - leafBoundaries(shardIndex).dictionaryPos, shardIndex, offset - 1)
  }

  def extract(columns: Seq[String], docID: Int): Row = {
    if (dvMap.size > 0) {
      val shardIndex = readerIndex(docID)
      val sqlFields = columns.map((column) => {
        if (converter.dimensions.contains(column))
          extractDimension(docID, shardIndex, column)
        else if (converter.types.contains(column))
          extractMeasure(docID, shardIndex, column)
        else throw new LuceneDAOException(s"unsupported ${column} in doc value extraction")
      })
      Row.fromSeq(sqlFields)
    } else {
      Row.empty
    }
  }

  def getOffset(column: String, docID: Int): Int = {
    val shardIndex = readerIndex(docID)
    dvMap(column).getOffset(docID - leafBoundaries(shardIndex).dictionaryPos, shardIndex)
  }

  def extract(column: String, docID: Int, offset: Int): Any = {
    val shardIndex = readerIndex(docID)
    val dv = dvMap(column).extract(docID - leafBoundaries(shardIndex).dictionaryPos, shardIndex, offset)
    dv
  }
}

object DocValueExtractor {
  def apply(leafReaders: Seq[LuceneReader],
            converter: OLAPConverter): DocValueExtractor = {
    new DocValueExtractor(leafReaders, converter)
  }
}
