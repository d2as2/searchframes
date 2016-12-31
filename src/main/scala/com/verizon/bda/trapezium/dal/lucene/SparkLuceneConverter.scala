package com.verizon.bda.trapezium.dal.lucene

/**
 * @author pramod.lakshminarasimha Spark Lucene Converterfor stored fields
 *         15 Dec 2016 debasish83 Converter for OLAP queries supporting indexed, DocValues and stored fields
 */

import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.util.BytesRef
import org.apache.spark.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document._
import java.sql.Timestamp

trait SparkLuceneConverter extends Serializable with Logging {

  def rowToDoc(r: Row): Document

  def docToRow(d: Document): Row

  def schema: StructType

  val ser: SerializerInstance

  // Solr / Elastisearch uses schema.xml to achieve this while trapezium supports SparkSQL types
  def toIndexedField(name: String,
                     dataType: DataType,
                     value: Any,
                     store: Field.Store = Field.Store.NO): Field = {
    dataType match {
      // String is saved as standard reverse index from search engines
      case s: StringType =>
        new StringField(name, value.asInstanceOf[String], store)
      // On integer, long, float and double we do want to push range queries and indexing distinct
      // value makes no sense
      case i: IntegerType =>
        new IntField(name, value.asInstanceOf[Int], store)
      case l: LongType =>
        new LongField(name, value.asInstanceOf[Long], store)
      case f: FloatType =>
        new FloatField(name, value.asInstanceOf[Float], store)
      case d: DoubleType =>
        new DoubleField(name, value.asInstanceOf[Double], store)
      case _ =>
        throw new LuceneDAOException(s"unsupported sparksql ${dataType} for indexed field")
    }
  }

  def toDocValueField(name: String,
                      dataType: DataType,
                      multivalued: Boolean,
                      value: Any): Field = {
    val field = dataType match {
      case i: IntegerType =>
        new NumericDocValuesField(name, value.asInstanceOf[Int])
      case l: LongType =>
        new NumericDocValuesField(name, value.asInstanceOf[Long])
      case f: FloatType =>
        new FloatDocValuesField(name, value.asInstanceOf[Float])
      case d: DoubleType =>
        new DoubleDocValuesField(name, value.asInstanceOf[Long])
      case dt: TimestampType =>
        new NumericDocValuesField(name, value.asInstanceOf[Timestamp].getTime)
      case st: StringType =>
        val bytes = ser.serialize(value).array()
        new SortedDocValuesField(name, new BytesRef(bytes))
      case _ => logInfo(s"serializing ${dataType.typeName} as binary doc value field")
        val bytes = ser.serialize(value).array()
        new BinaryDocValuesField(name, new BytesRef(bytes))
    }

    if (multivalued) {
      assert(dataType == IntegerType, "multi-valued dimensions must be integer")
      new SortedNumericDocValuesField(name, value.asInstanceOf[Int])
    }
    else field
  }

  def toStoredField(name: String,
                    dataType: DataType,
                    value: Any): Field = {
    new StoredField(name, ser.serialize(value).array())
  }
}
