package com.verizon.bda.trapezium.dal.lucene

import java.sql.Timestamp
import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.index._
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types._
import java.nio.ByteBuffer
import org.apache.spark.sql.Row

/**
 * @author debasish83 on 12/22/16.
 *         Supports primitives for extracting doc value fields
 */
class DocValueExtractor(leafReader: LeafReader,
                        converter: OLAPConverter) {
  val dvMap = converter.types.map { case (k, v) =>
    if (v.multiValued) (k, DocValues.getSortedSet(leafReader, k))
    else {
      v.dataType match {
        case i: IntegerType => (k, DocValues.getNumeric(leafReader, k))
        case l: LongType => (k, DocValues.getNumeric(leafReader, k))
        case f: FloatType => (k, DocValues.getNumeric(leafReader, k))
        case d: DoubleType => (k, DocValues.getNumeric(leafReader, k))
        case dt: TimestampType => (k, DocValues.getNumeric(leafReader, k))
        case st: StringType => (k, DocValues.getSorted(leafReader, k))
        case _ => (k, DocValues.getBinary(leafReader, k))
      }
    }
  }

  //TODO: Before going to performance opt, let's expose it out as a RDD[Row] for df operations

  //TODO: We are bringing out-heap memory to in-heap now which affects performance
  def extractMeasure(docID: Int, column: String): Any = {
    val measureType = converter.types(column).dataType
    measureType match {
      case i: IntegerType => dvMap(column).asInstanceOf[NumericDocValues].get(docID)
      case l: LongType => dvMap(column).asInstanceOf[NumericDocValues].get(docID)
      case f: FloatType => dvMap(column).asInstanceOf[NumericDocValues].get(docID).toFloat
      case d: DoubleType => dvMap(column).asInstanceOf[NumericDocValues].get(docID).toDouble
      case dt: TimestampType => new Timestamp(dvMap(column).asInstanceOf[NumericDocValues].get(docID))
      case st: StringType =>
        val bytes = dvMap(column).asInstanceOf[SortedDocValues].get(docID).bytes
        converter.ser.deserialize[String](ByteBuffer.wrap(bytes))
      case v: VectorUDT =>
        val bytes = dvMap(column).asInstanceOf[SortedDocValues].get(docID).bytes
        converter.ser.deserialize[Vector](ByteBuffer.wrap(bytes))
      case _ =>
        throw new LuceneDAOException(s"unsupported serialization for column ${column} type ${measureType}")
    }
  }

  //TODO: We are bringing out-heap memory to in-heap now which should affects performance.
  // Here we do want to push the aggregation down
  def extractDimension(docID: Int, column: String): Any = {
    val dimension = if (converter.types(column).multiValued) {
      val multiDimDV = dvMap(column).asInstanceOf[SortedSetDocValues]
      val maxIdx = multiDimDV.getValueCount
      val indices = Array.fill[Int](maxIdx.toInt)(0)
      var i = 0
      while (i < maxIdx) {
        val bytes = multiDimDV.lookupOrd(i).bytes
        val idx = converter.ser.deserialize[Int](ByteBuffer.wrap(bytes))
        indices(i) = idx
        i += 1
      }
    } else {
      val dimDV = dvMap(column).asInstanceOf[NumericDocValues]
      dimDV.get(docID).toInt
    }
    dimension
  }

  def extract(docID: Int, columns: Seq[String]): Row = {
    val sqlFields = columns.map((column) => {
      if (converter.dimensions.contains(column)) extractDimension(docID, column)
      else if (converter.types.contains(column)) extractMeasure(docID, column)
      else throw new LuceneDAOException(s"unsupported ${column} in doc value extraction")
    })
    Row(sqlFields)
  }
}

object DocValueExtractor {
  def apply(leafReader: LeafReader,
            converter: OLAPConverter): DocValueExtractor = {
    new DocValueExtractor(leafReader, converter)
  }
}
