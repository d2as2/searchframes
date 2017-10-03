package com.verizon.bda.trapezium.dal.lucene

import java.util.UUID

import org.apache.lucene.document.{Document, Field}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.util.CustomVectorUDT
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author debasish83 on 12/22/16.
 *         Converters for dataframe to OLAP compatible column stores
 */

// TODO: Given a dataframe schema create all the Projection
trait SparkSQLProjections {
  @transient lazy val VectorProjection = UnsafeProjection.create(Array(VectorType.asInstanceOf[DataType]))
  // CHeck [SPARK-16074] for usage here for VectorUDT
  lazy val VectorType = new CustomVectorUDT()
  lazy val unsafeRow = new UnsafeRow()
}

// Dimensions needs to be indexed, dictionary-mapped and docvalued
// measures should be docvalued
// The types on dimension and measures must be SparkSQL compatible
// Dimensions are a subset of types.keySet
class OLAPConverter(val dimensions: Set[String],
val storedDimensions: Set[String],
val measures: Set[String],
val serializer: KryoSerializer) extends SparkLuceneConverter {
  @transient lazy val ser = serializer.newInstance()

  @transient private var log = LoggerFactory.getLogger(this.getClass)

  def getLog(): Logger = {
    if(log==null)
      log = LoggerFactory.getLogger(this.getClass)

    log
  }

  def addField(doc: Document,
               fieldName: String,
               dataType: DataType,
               value: Any,
               multiValued: Boolean): Unit = {
    // dimensions will be indexed and docvalued based on dictionary encoding
    // measures will be docvalued
    if (value == null) return

    if (dimensions.contains(fieldName)) {
      doc.add(toIndexedField(fieldName, dataType, value, Field.Store.NO))
    } else if (storedDimensions.contains(fieldName)) {
      doc.add(toIndexedField(fieldName, dataType, value, Field.Store.NO))
      // Dictionary encoding on dimension doc values
      val feature = value.asInstanceOf[String]
      val idx = dict.indexOf(fieldName, feature)
      // SingleValue and MultiValue dimension are both stored as NumericSet
      doc.add(toDocValueField(fieldName, IntegerType, true, idx))
    }
    else if (measures.contains(fieldName)) {
      doc.add(toDocValueField(fieldName, dataType, multiValued, value))
    }
  }

  private var inputSchema: StructType = _

  private var dict: DictionaryManager = _

  def setSchema(schema: StructType): OLAPConverter = {
    getLog().debug(s"schema ${schema} set for the converter")
    this.inputSchema = schema
    this
  }

  def setDictionary(dict: DictionaryManager): OLAPConverter = {
    log.info(s"Setting dictionary of size ${dict.size()} for converter")
    this.dict = dict
    this
  }

  def rowToDoc(row: Row): Document = {
    val doc = new Document()
    // Add unique UUID for SolrCloud push
    doc.add(toIndexedField("uuid", StringType, UUID.randomUUID().toString, Field.Store.YES))
    inputSchema.fields.foreach(field => {
      val fieldName = field.name
      val fieldIndex = row.fieldIndex(fieldName)
      val fieldValue = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
      // TODO: How to handle None values
      if (fieldValue.isDefined) {
        val value = fieldValue.get
        field.dataType match {
          case at: ArrayType =>
            val it = value.asInstanceOf[Iterable[Any]].iterator
            while (it.hasNext) addField(doc, fieldName, at.elementType, it.next(), true)
          case _ =>
            addField(doc, fieldName, field.dataType, value, false)
        }
      }
    })
    doc
  }

  private var columns: Seq[String] = _

  def setColumns(columns: Seq[String]): OLAPConverter = {
    this.columns = columns
    this
  }

  // Use docToRow to extract stored field since the stored fields are part of doc
  def docToRow(doc: Document): Row = {
    Row.empty
  }

  override def schema: StructType = inputSchema

}

object OLAPConverter {
  def apply(dimensions: Set[String],
            storedDimensions: Set[String],
            measures: Set[String]): OLAPConverter = {
    val ser = new KryoSerializer(new SparkConf())
    new OLAPConverter(dimensions, storedDimensions, measures, ser)
  }
}
