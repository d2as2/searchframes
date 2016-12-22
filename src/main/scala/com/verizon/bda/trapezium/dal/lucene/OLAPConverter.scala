package com.verizon.bda.trapezium.dal.lucene

import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.document.Document
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.control.Breaks._

/**
 * @author debasish83 on 12/22/16.
 *         Converters for dataframe to OLAP compatible column stores
 */

case class LuceneType(multiValued: Boolean, dataType: DataType)

//dimensions needs to be indexed, dictionary-mapped and docvalued
//measures should be docvalued
//the types on dimension and measures must be SparkSQL compatible
//dimensions are a subset of types.keySet
class OLAPConverter(val conf: SparkConf,
                    val dimensions: Set[String],
                    val types: Map[String, LuceneType]) extends SparkLuceneConverter {
  //TODO: Use SparkSQL Expression encoder in place of Kryo to serialize/deserialize to native UnsafeRow
  // val encoder: ExpressionEncoder[Row] = RowEncoder(dfSchema)

  val ser = new KryoSerializer(conf).newInstance()

  def addField(doc: Document,
               fieldName: String,
               dataType: DataType,
               value: Any,
               multiValued: Boolean): Unit = {
    // dimensions will be indexed and docvalued based on dictionary encoding
    // measures will be docvalued
    // TODO: dictionary encoding
    if (dimensions.contains(fieldName)) {
      doc.add(toIndexedField(fieldName, dataType, value))
      doc.add(toDocValueField(fieldName, dataType, multiValued, value))
    }
    else if (types.contains(fieldName))
      doc.add(toDocValueField(fieldName, dataType, multiValued, value))
    else {
      logInfo(s"${fieldName} is not in dimensions/measures")
      doc.add(toStoredField(fieldName, dataType, value))
    }
  }

  private var inputSchema: StructType = _

  def setSchema(schema: StructType): OLAPConverter = {
    this.inputSchema = schema
    this
  }

  def rowToDoc(row: Row): Document = {
    val doc = new Document()
    inputSchema.fields.foreach(field => {
      val fieldName = field.name
      breakable {
        if (fieldName.equals("_version_")) break()
        val fieldIndex = row.fieldIndex(fieldName)

        val fieldValue = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        //TODO: How to handle None values
        if (fieldValue.isDefined) {
          val value = fieldValue.get
          field.dataType match {
            case at: ArrayType =>
              val it = value.asInstanceOf[Iterable[Any]].iterator
              while (it.hasNext) addField(doc, fieldName, at.elementType, value, true)
            case _ => addField(doc, fieldName, field.dataType, value, false)
          }
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
  def apply(conf: SparkConf,
            dimensions: Set[String],
            types: Map[String, LuceneType]): OLAPConverter = {
    new OLAPConverter(conf, dimensions, types)
  }
}
