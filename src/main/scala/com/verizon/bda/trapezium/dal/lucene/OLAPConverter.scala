package com.verizon.bda.trapezium.dal.lucene

import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.document.Document
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
case class LuceneType(multiValued: Boolean, dataType: DataType)

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
                    val types: Map[String, LuceneType],
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

    if (dimensions.contains(fieldName)) {
      doc.add(toIndexedField(fieldName, dataType, value))
      // Dictionary encoding on dimension doc values
      val feature = value.asInstanceOf[String]
      val idx = dict.indexOf(fieldName, feature)
      doc.add(toDocValueField(fieldName, IntegerType, multiValued, idx))
    }
    else if (types.contains(fieldName)) {
      doc.add(toDocValueField(fieldName, dataType, multiValued, value))
    }
    else {
      logInfo(s"${fieldName} is not in dimensions/measures")
      doc.add(toStoredField(fieldName, dataType, value))
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

  override def schema: StructType = {
    val sparkTypes = columns.map((column) => {
      if (dimensions.contains(column)) {
        // multivalue dimension
        assert(types(column).dataType == IntegerType, s"dimension ${column} is not integer type")
        if (types(column).multiValued) StructField(column, ArrayType(IntegerType), false)
        else StructField(column, IntegerType, false)
      } else if (types.contains(column)) {
        StructField(column, types(column).dataType)
      } else {
        throw new LuceneDAOException("query columns are not dimension/measure")
      }
    })
    StructType(sparkTypes)
  }
}

object OLAPConverter {
  def apply(dimensions: Set[String],
            types: Map[String, LuceneType]): OLAPConverter = {
    val ser = new KryoSerializer(new SparkConf())
    new OLAPConverter(dimensions, types, ser)
  }
}
