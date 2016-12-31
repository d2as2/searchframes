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

abstract class DocValueAccessor(leafReaders: Seq[LeafReader],
                                fieldName: String) extends Serializable {
  def extract(docID: Int, shardIndex: Int, offset: Int): Any

  //doc value offset for a given accessor, default offset is 1
  def getOffset(docID: Int, shardIndex: Int): Int

  //local per partition encoding
  //def size(): size of the doc value vector
  //def distinct(): distinct elements in doc value vector
  //def encode(localIndex: Int): Int generate global index from local index
}

//TODO: May want to break into a class per dataType if the case matching has
//TODO: performance implications
class NumericAccessor(leafReaders: Seq[LeafReader],
                      fieldName: String)
  extends DocValueAccessor(leafReaders, fieldName) {

  val docValueReaders = leafReaders.map(DocValues.getNumeric(_, fieldName))

  def getOffset(docID: Int, shardIndex: Int): Int = 1

  def extract(docID: Int, shardIndex: Int, offset: Int): Any = {
    assert(offset == 0, s"numeric docvalue accessor non-zero offset $offset")
    docValueReaders(shardIndex).get(docID)
  }
}

class StringAccessor(leafReaders: Seq[LeafReader],
                     fieldName: String,
                     ser: SerializerInstance)
  extends DocValueAccessor(leafReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSorted(_, fieldName))

  def getOffset(docID: Int, shardIndex: Int): Int = 1

  def extract(docID: Int, shardIndex: Int, offset: Int): Any = {
    assert(offset == 0, s"string docvalue accessor non-zero offset $offset")
    val bytes = docValueReaders(shardIndex).get(docID).bytes
    ser.deserialize[String](ByteBuffer.wrap(bytes))
  }
}

class BinaryAccessor[T: ClassTag](leafReaders: Seq[LeafReader],
                     fieldName: String,
                     dataType: DataType,
                     ser: SerializerInstance)
  extends DocValueAccessor(leafReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getBinary(_, fieldName))

  def getOffset(docID: Int, shardIndex: Int): Int = 1

  def extract(docID: Int, shardIndex: Int, offset: Int): Any = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    val bytes = docValueReaders(shardIndex).get(docID).bytes
    ser.deserialize[T](ByteBuffer.wrap(bytes))
  }
}

class ArrayNumericAccessor(leafReaders: Seq[LeafReader],
                           fieldName: String)
  extends DocValueAccessor(leafReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSortedNumeric(_, fieldName))

  def getOffset(docID: Int, shardIndex: Int): Int = {
    docValueReaders(shardIndex).setDocument(docID)
    docValueReaders(shardIndex).count()
  }

  def extract(docID: Int, shardIndex: Int, offset: Int): Any = {
    docValueReaders(shardIndex).setDocument(docID)
    docValueReaders(shardIndex).valueAt(offset)
  }
}

object DocValueAccessor extends Serializable {
  def apply(leafReaders: Seq[LeafReader],
            fieldName: String,
            dataType: DataType,
            multiValued: Boolean,
            ser: SerializerInstance): DocValueAccessor = {
    //TODO: Test multivalue time stamp column for optimizing document size
    if (multiValued) new ArrayNumericAccessor(leafReaders, fieldName)
    else {
      dataType match {
        case i: IntegerType => new NumericAccessor(leafReaders, fieldName)
        case l: LongType => new NumericAccessor(leafReaders, fieldName)
        case f: FloatType => new NumericAccessor(leafReaders, fieldName)
        case d: DoubleType => new NumericAccessor(leafReaders, fieldName)
        case ts: TimestampType => new NumericAccessor(leafReaders, fieldName)
        case st: StringType => new StringAccessor(leafReaders, fieldName, ser)
        case v: VectorUDT => new BinaryAccessor[Vector](leafReaders, fieldName, dataType, ser)
        case _ => throw new LuceneDAOException(s"unsupported type ${dataType} for docvalue accessor constructor")
      }
    }
  }
}
