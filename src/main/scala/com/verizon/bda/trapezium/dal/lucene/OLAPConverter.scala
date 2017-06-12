package com.verizon.bda.trapezium.dal.lucene

import org.apache.log4j.Logger
import org.apache.lucene.document.{Field, Document}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._

/**
 * @author debasish83 on 12/22/16.
 *         Converters for dataframe to OLAP compatible column stores
 */

// TODO: Given a dataframe schema create all the Projection
trait SparkSQLProjections {
  @transient lazy val VectorProjection = UnsafeProjection.create(VectorType.sqlType)

  lazy val VectorType = new VectorUDT()
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

  def addField(doc: Document,
               fieldName: String,
               dataType: DataType,
               value: Any,
               multiValued: Boolean): Unit = {
    // dimensions will be indexed and docvalued based on dictionary encoding
    // measures will be docvalued
    if (value == null) return

    if (dimensions.contains(fieldName) || storedDimensions.contains(fieldName)) {
      // create indexed field
      if (dimensions.contains(fieldName)) {
        doc.add(toIndexedField(fieldName, dataType, value, Field.Store.NO))
      } else {
        doc.add(toIndexedField(fieldName, dataType, value, Field.Store.NO))
        //dictionary encoding on dimension doc values
        val feature = value.asInstanceOf[String]
        val idx = dict.indexOf(fieldName, feature)
        if (idx == -1) {
          logError(s"Doc feature ${fieldName}:${feature} not found in the dictionary")
        } else {
          doc.add(toDocValueField(fieldName, IntegerType, multiValued, idx))
        }
      }
    } else if (measures.contains(fieldName)) {
      logInfo(s"${fieldName} is not in dimensions")
      //TODO: Add a config to choose row based or column based storage
      //doc.add(toStoredField(fieldName, dataType, value))
      doc.add(toDocValueField(fieldName, dataType, multiValued, value))
    }
  }

  private var inputSchema: StructType = _

  private var dict: DictionaryManager = _

  def setSchema(schema: StructType): OLAPConverter = {
    logDebug(s"schema ${schema} set for the converter")
    this.inputSchema = schema
    this
  }

  def setDictionary(dict: DictionaryManager): OLAPConverter = {
    logInfo(s"Setting dictionary of size ${dict.size()} for converter")
    this.dict = dict
    this
  }

  def rowToDoc(row: Row): Document = {
    val doc = new Document()
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
