package com.verizon.bda.trapezium.dal.lucene

import java.nio.ByteBuffer
import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.index.{DocValues, LeafReader}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 * @author debasish83 on 12/24/16.
 *        SparkSQL ColumnAccessor pattern for disk backed / in-memory doc values
 *        No need to call dataframe.cache to build the columnar compression, the
 *        data is backed on disk using columnar compression during index time in LuceneDAO
 */

abstract class DocValueAccessor(leafReader: LeafReader,
                                fieldName: String) extends Serializable {
  def extract(docID: Int, offset: Int): Any

  //doc value offset for a given accessor, default offset is 1
  def getOffset(docID: Int): Int

  //local per partition encoding
  //def size(): size of the doc value vector
  //def distinct(): distinct elements in doc value vector
  //def encode(localIndex: Int): Int generate global index from local index
}

//TODO: May want to break into a class per dataType if the case matching has
//TODO: performance implications
class NumericAccessor[T](leafReader: LeafReader,
                      fieldName: String)
  extends DocValueAccessor(leafReader, fieldName) {

  val docValueReader = DocValues.getNumeric(leafReader, fieldName)

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"numeric docvalue accessor non-zero offset $offset")
    docValueReader.get(docID).asInstanceOf[T]
  }
}

class StringAccessor(leafReader: LeafReader,
                     fieldName: String,
                     ser: SerializerInstance)
  extends DocValueAccessor(leafReader, fieldName) {
  val docValueReader = DocValues.getSorted(leafReader, fieldName)

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"string docvalue accessor non-zero offset $offset")
    val bytes = docValueReader.get(docID).bytes
    ser.deserialize[String](ByteBuffer.wrap(bytes))
  }
}

class BinaryAccessor[T: ClassTag](leafReader: LeafReader,
                     fieldName: String,
                     dataType: DataType,
                     ser: SerializerInstance)
  extends DocValueAccessor(leafReader, fieldName) {
  val docValueReader = DocValues.getBinary(leafReader, fieldName)

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    val bytes = docValueReader.get(docID).bytes
    ser.deserialize[T](ByteBuffer.wrap(bytes))
  }
}

class ArrayNumericAccessor(leafReader: LeafReader,
                           fieldName: String)
  extends DocValueAccessor(leafReader, fieldName) {
  val docValueReader = DocValues.getSortedNumeric(leafReader, fieldName)

  def getOffset(docID: Int): Int = {
    docValueReader.setDocument(docID)
    docValueReader.count()
  }

  def extract(docID: Int, offset: Int): Any = {
    docValueReader.setDocument(docID)
    docValueReader.valueAt(offset).toInt
  }
}

object DocValueAccessor extends Serializable {
  def apply(leafReader: LeafReader,
            fieldName: String,
            dataType: DataType,
            multiValued: Boolean,
            ser: SerializerInstance): DocValueAccessor = {
    //TODO: Test multivalue time stamp column for optimizing document size
    if (multiValued) new ArrayNumericAccessor(leafReader, fieldName)
    else {
      dataType match {
        case i: IntegerType => new NumericAccessor[Int](leafReader, fieldName)
        case l: LongType => new NumericAccessor[Long](leafReader, fieldName)
        case f: FloatType => new NumericAccessor[Float](leafReader, fieldName)
        case d: DoubleType => new NumericAccessor[Double](leafReader, fieldName)
        case ts: TimestampType => new NumericAccessor[Long](leafReader, fieldName)
        case st: StringType => new StringAccessor(leafReader, fieldName, ser)
        case v: VectorUDT => new BinaryAccessor[Vector](leafReader, fieldName, dataType, ser)
        case _ => throw new LuceneDAOException(s"unsupported type ${dataType} for docvalue accessor constructor")
      }
    }
  }
}
