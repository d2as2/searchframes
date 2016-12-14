package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.document.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.sources.{InsertableRelation, TableScan, BaseRelation}
import org.apache.spark.sql.types._

/**
 * @author debasish83 on 12/11/16.
 */
/*
  dimensions can be sparse/dense,
  if dense it should give index based on fieldName
  if sparse it should give Seq[indices] based on fieldName
  if sparse it should give Seq[indices] based on fieldName
  measures is a pre-specified data type based on fieldName
*/
class LuceneRelation(dimensions: Seq[String],
                     measures: Seq[String],
                     ctx: SQLContext)
  extends BaseRelation
  with SparkLuceneConverter
  with TableScan
  with InsertableRelation
  with Serializable
  with Logging {

  override def sqlContext: SQLContext = ctx

  override def schema: StructType = { ??? }

  def toLuceneType(dataType: DataType): String = {
    case bi: BinaryType => "binary"
    case b: BooleanType => "boolean"
    case dt: DateType => "tdate"
    case db: DoubleType => "tdouble"
    case dec: DecimalType => "tdouble"
    case ft: FloatType => "tfloat"
    case i: IntegerType => "tint"
    case l: LongType => "tlong"
    case s: ShortType => "tint"
    case t: TimestampType => "tdate"
    case _ => "string"
  }

  def toAddFieldMap(sf: StructField): Map[String, AnyRef] = {
    val map = scala.collection.mutable.Map[String, AnyRef]()
    map += ("name" -> sf.name)
    map += ("indexed" -> "true")
    map += ("stored" -> "true")
    map += ("docValues" -> "true")
    val dataType = sf.dataType
    dataType match {
      case at: ArrayType =>
        map += ("multiValued" -> "true")
        map += ("type" -> toLuceneType(at.elementType))
      case _ =>
        map += ("multiValued" -> "false")
        map += ("type" -> toLuceneType(dataType))
    }
    map.toMap
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

  }

  override def rowToDoc(r: Row): Document = {
    ???
  }

  override def docToRow(d: Document): Row = {
    ???
  }

  override def buildScan(): RDD[Row] = {
    ???
  }
}
