package com.verizon.bda.trapezium.dal.lucene

/**
 * @author pramod.lakshminarasimha
 *         15 Dec 2016 debasish83 Converter for OLAP queries
 */

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{IntField, StringField, Document}
import org.apache.lucene.document.Field

trait SparkLuceneConverter extends Serializable {

  def rowToDoc(r: Row): Document

  def docToRow(d: Document): Row

  val schema: StructType

}

class OLAPConverter(dimension: Map[String, Boolean],
                    measures: Map[String, StructType]) extends SparkLuceneConverter {
  def rowToDoc(r: Row): Document = {
    val d = new Document()
    schema.foreach {
      case StructField(name: String, StringType, _, _) => d.add(new StringField(name,
        r.getAs[String](name), Field.Store.YES))

      case StructField(name: String, ArrayType(StringType, _), _, _) =>
        r.getList[String](tldIndex).asScala.foreach { s =>
          d.add(new StringField(name, s, Field.Store.YES))
        }

      case StructField(name: String, ArrayType(IntegerType, _), _, _) =>
        r.getList[Int](indicesIndex).asScala.foreach { i =>
          d.add(new IntField(name, i, Field.Store.YES))
        }
    }
    ???
  }

  def docToRow(d: Document): Row = {
    ???
  }

  var columns: Seq[String] = _

  def setColumns(columns: Seq[String]): OLAPConverter = {
    this.columns = columns
    this
  }

  val schema: StructType = {
    assert(columns != null)
    columns.map{case(column) =>
      if (dimension.contains(column)) {
        StructField
      }
    }
  }
}