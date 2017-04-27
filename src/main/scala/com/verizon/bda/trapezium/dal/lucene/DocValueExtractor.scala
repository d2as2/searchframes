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
  val schema = converter.schema
  val dimensions = converter.dimensions
  val storedDimensions = converter.storedDimensions
  val measures = converter.measures
  val ser = converter.ser

  private val dvMap: Map[String, DocValueAccessor] = if (leafReaders.length > 0) {
    schema.toSeq.map{ field: StructField =>
      val fieldName = field.name
      val fieldType =
        if (dimensions.contains(fieldName) || storedDimensions.contains(fieldName)) IntegerType
        else field.dataType
      val fieldMultiValued = (field.dataType.isInstanceOf[ArrayType])
      val accessor = DocValueAccessor(leafReaders, fieldName, fieldType, fieldMultiValued, ser)
      (fieldName -> accessor)
    }.toMap
  } else {
    Map.empty[String, DocValueAccessor]
  }

  //TODO: Measure can be multi-valued as well. for first iteration of time series
  //TODO: measures are considered to be single-valued
  private def extractMeasure(docID: Int, column: String): Any = {
    assert(measures.contains(column), s"$column is not a measure")
    val offset = dvMap(column).getOffset(docID)
    assert(offset == 1, s"measure $column is a multi-value field with offset $offset")
    dvMap(column).extract(docID, offset - 1)
  }

  private def extractStoredDimension(docID: Int, column: String): Any = {
    assert(storedDimensions.contains(column), s"$column is not a dimension")
    val offset = dvMap(column).getOffset(docID)
    if (offset == 1)
      dvMap(column).extract(docID, 0)
    else if (offset > 1)
      Seq((0 until offset).map(dvMap(column).extract(docID, _)): _*)
    else if (offset == 0)
      -1L // this happens only when the ArrayNumeric field is Null. return -1L as dummy feature index
  }

  // only storedDimensions and measures can be extracted
  def extract(columns: Seq[String], docID: Int): Row = {
    if (dvMap.size > 0) {
      val sqlFields = columns.map((column) => {
        if (storedDimensions.contains(column))
          extractStoredDimension(docID, column)
        else if (measures.contains(column))
          extractMeasure(docID, column)
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
