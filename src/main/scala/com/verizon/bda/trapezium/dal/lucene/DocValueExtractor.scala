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
  val searchDimensions = converter.searchDimensions
  val ser = converter.ser

  private val dvMap: Map[String, DocValueAccessor] = if (leafReaders.length > 0) {
    types.filterNot {
      case (k, _) => searchDimensions.contains(k)
    }.map { case (k, v) =>
      // Dimensions have gone through DictionaryEncoding and uses sortedsetnumeric storage
      val accessor = if (dimensions.contains(k)) {
        DocValueAccessor(leafReaders, k, IntegerType, true, ser)
      } else {
        DocValueAccessor(leafReaders, k, v.dataType, v.multiValued, ser)
      }
      (k, accessor)
    }
  } else {
    Map.empty[String, DocValueAccessor]
  }
  
  // TODO: Measure can be multi-valued as well. for first iteration of time series
  // TODO: measures are considered to be single-valued
  private def extractMeasure(docID: Int, column: String): Any = {
    assert(!dimensions.contains(column), s"$column is not a measure")
    val offset = dvMap(column).getOffset(docID)
    assert(offset == 1, s"measure $column is a multi-value field with offset $offset")
    dvMap(column).extract(docID, offset - 1)
  }

  private def extractDimension(docID: Int, column: String): Any = {
    assert(dimensions.contains(column), s"$column is not a dimension")
    val offset = dvMap(column).getOffset(docID)
    if (offset > 1) Seq((0 until offset).map(dvMap(column).extract(docID, _)): _*)
    else dvMap(column).extract(docID, offset - 1)
  }

  def extract(columns: Seq[String], docID: Int): Row = {
    if (dvMap.size > 0) {
      val sqlFields = columns.map((column) => {
        if (converter.dimensions.contains(column)) extractDimension(docID, column)
        else if (converter.types.contains(column)) extractMeasure(docID, column)
        else throw new LuceneDAOException(s"unsupported ${column} in doc value extraction")
      })
      Row.fromSeq(sqlFields)
    } else {
      Row.empty
    }
  }

  def getOffset(column: String, docID: Int): Int = {
    dvMap(column).getOffset(docID)
  }

  def extract(column: String, docID: Int, offset: Int): Any = {
    dvMap(column).extract(docID, offset)
  }
}

object DocValueExtractor {
  def apply(leafReaders: Seq[LuceneReader],
            converter: OLAPConverter): DocValueExtractor = {
    new DocValueExtractor(leafReaders, converter)
  }
}
