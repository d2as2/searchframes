package com.verizon.bda.trapezium.dal.lucene

import java.nio.ByteBuffer
import java.sql.Timestamp

import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.index.{DocValues, LeafReader}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.types._

/**
 * @author debasish83 on 12/24/16.
 *        SparkSQL ColumnAccessor pattern for disk backed / in-memory doc values
 *        No need to call dataframe.cache to build the columnar compression, the
 *        data is backed on disk using columnar compression during index time in LuceneDAO
 */

/*
  For one group
  StringAccessor.aggregate(aggregator, docID, ArrayAccessor) {
  Here we want ArrayAccessor to extract the indices but don't want to use
  additional memory than what ArrayAccessor already has been allocated
  ArrayAccessor.extract(docID): Int
      def hasNext: Boolean

      Now what happen if ArrayAccessor.extractTo is called by another thread ?

      It can't happen since we have one LuceneShard per partition, no docValues are shared
      within thread

      Inside the loop we will call this.extract(docID) while hasNext
  }

  For multiple group
  StringAccesor.
  aggregate(aggregator,
  docID, Seq[ArrayAccessor, IntAccessor, IntAccessor])

  Dimension is ArrayAccessor we get range of indices
  Now for Aggregator we need to define a data type
  Sum extends Aggregator
  HyperLogLog extends Aggregator

  May be it is:

  Aggregator(Seq[ArrayAccessor, IntAccessor, IntAccessor]) {
    aggregate(Seq[docIDs], StringAccessor) : InternalRow
    merge(InternalRow, InternalRow): InternalRow
  }

  Say we want to use Catalyst aggregator

  Aggregator {
    def initialize(size = ArrayAccessor.size * IntAccessor.size * IntAccessor.size): Unit
    This initializes a buffer of size with InternalRow[Any]

    def update(docID: Int, st: StringAccessor): Unit {
      ArrayAccessor.extract(docID)
      IntAccessor.extract(docID)
      while(ArrayAccessor.hasNext) {
        while(IntAccesor.hasNext) {
          val id1 = ArrayAccesor.extract(docID)
          val id2 = IntAccesor.extract(docID)
          val index = f(id1, id2)
          buffer.update(
          buffer(index) += T(st.extract(docID))
  }

  def merge(buffer1: InternalRow, buffer2: InternalRow): Unit

  Can we aggregate more than one measure at once ?

  Aggregator.aggregate(docIDs, StringAccessor)
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
class NumericAccessor(leafReader: LeafReader,
                      fieldName: String,
                      dataType: DataType)
  extends DocValueAccessor(leafReader, fieldName) {

  val docValueReader = DocValues.getNumeric(leafReader, fieldName)

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"numeric docvalue accessor non-zero offset $offset")
    val data = docValueReader.get(docID)
    dataType match {
      case i: IntegerType => data.toInt
      case i: LongType => data
      case f: FloatType => data.toFloat
      case d: DoubleType => data.toDouble
      case dt: TimestampType => new Timestamp(data)
      case _ => new LuceneDAOException(s"unsupported ${dataType} in numeric accessor")
    }
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

class BinaryAccessor(leafReader: LeafReader,
                     fieldName: String,
                     dataType: DataType,
                     ser: SerializerInstance)
  extends DocValueAccessor(leafReader, fieldName) {
  val docValueReader = DocValues.getBinary(leafReader, fieldName)

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    assert(dataType == new VectorUDT, s"unsupported $dataType in binary accessor ")
    val bytes = docValueReader.get(docID).bytes
    ser.deserialize[Vector](ByteBuffer.wrap(bytes))
  }
}

//TODO: May want to break into a class per dataType if the case matching has
//TODO: performance implications
class ArrayNumericAccessor(leafReader: LeafReader,
                           fieldName: String,
                           dataType: DataType)
  extends DocValueAccessor(leafReader, fieldName) {

  val docValueReader = DocValues.getSortedNumeric(leafReader, fieldName)

  def getOffset(docID: Int): Int = {
    docValueReader.setDocument(docID)
    docValueReader.count()
  }

  def extract(docID: Int, offset: Int): Any = {
    docValueReader.setDocument(docID)
    val data = docValueReader.valueAt(offset)
    dataType match {
      case i: IntegerType => data.toInt
      case i: LongType => data
      case f: FloatType => data.toFloat
      case d: DoubleType => data.toDouble
      case dt: TimestampType => new Timestamp(data)
      case _ => new LuceneDAOException(s"unsupported ${dataType} in numeric array accessor")
    }
  }
}

object DocValueAccessor extends Serializable {
  def apply(leafReader: LeafReader,
            fieldName: String,
            dataType: DataType,
            multiValued: Boolean,
            ser: SerializerInstance): DocValueAccessor = {
    if (multiValued) new ArrayNumericAccessor(leafReader, fieldName, dataType)
    else {
      dataType match {
        case st: StringType => new StringAccessor(leafReader, fieldName, ser)
        case v: VectorUDT => new BinaryAccessor(leafReader, fieldName, dataType, ser)
        case _ => new NumericAccessor(leafReader, fieldName, dataType)
      }
    }
  }
}
