package com.verizon.bda.trapezium.dal.lucene

import java.nio.ByteBuffer
import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.lucene.index.{DocValues, LeafReader}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import scala.reflect.ClassTag

/**
 * @author debasish83 on 12/24/16.
 *        SparkSQL ColumnAccessor pattern for disk backed / in-memory doc values
 *        No need to call dataframe.cache to build the columnar compression, the
 *        data is backed on disk using columnar compression during index time in LuceneDAO
 */

trait DocValueLocator extends Serializable {
  val leafBoundaries: Seq[FeatureAttr]

  def locate(docID: Int): Int = {
    var i = 0
    while (i < leafBoundaries.length) {
      if (leafBoundaries(i).contains(docID)) return i
      i += 1
    }
    throw new LuceneDAOException(s"docID $docID not within " +
      s"leafBoundaries ${leafBoundaries.mkString(",")}")
  }

  def lower(index: Int): Int = {
    leafBoundaries(index).dictionaryPos
  }

  def upper(index: Int): Int = {
    lower(index) + leafBoundaries(index).featureOffset + 1
  }
}

abstract class DocValueAccessor(luceneReaders: Seq[LuceneReader],
                                fieldName: String)
  extends DocValueLocator {
  val leafBoundaries: Seq[FeatureAttr] = luceneReaders.map(_.range)

  val leafReaders: Seq[LeafReader] = luceneReaders.map(_.leafReader)

  def extract(docID: Int, offset: Int): Any

  // Doc value offset for a given accessor, default offset is 1
  def getOffset(docID: Int): Int = 1

  // Local per partition encoding
  // def size(): size of the doc value vector
  // def distinct(): distinct elements in doc value vector
  // def encode(localIndex: Int): Int generate global index from local index
}

// NumericAccessor work on Long even if incoming types are Int, Float, Long, Double
// Null numeric values are packed as 0
class NumericAccessor(luceneReaders: Seq[LuceneReader],
                      fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {

  val docValueReaders = leafReaders.map(DocValues.getNumeric(_, fieldName))

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"numeric docvalue accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    docValueReaders(shardIndex).get(docID - lower(shardIndex) + offset)
  }
}

// TODO: Null string in SortedDocValues is packed as -1
class StringAccessor(luceneReaders: Seq[LuceneReader],
                     fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSorted(_, fieldName))

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"string docvalue accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    val bytes = docValueReaders(shardIndex).get(docID - lower(shardIndex)).bytes
    val str = new String(bytes, "UTF-8")
    str
  }
}

// TODO: bytes must be non-null
class BinaryAccessor(luceneReaders: Seq[LuceneReader],
                     fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {

  val docValueReaders = leafReaders.map(DocValues.getBinary(_, fieldName))

  def extractBytes(docID: Int, offset: Int): Array[Byte] = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    docValueReaders(shardIndex).get(docID - lower(shardIndex)).bytes
  }

  override def extract(docID: Int, offset: Int): Any = {
    extractBytes(docID, offset)
  }
}

// TODO: single/multi-value dimensions should be packed as SortedNumericDocValues
// single dimension as numeric results in 0 for null which collapses with dimension index
class ArrayNumericAccessor(luceneReaders: Seq[LuceneReader],
                           fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSortedNumeric(_, fieldName))

  override def getOffset(docID: Int): Int = {
    val shardIndex = locate(docID)
    docValueReaders(shardIndex).setDocument(docID - lower(shardIndex))
    docValueReaders(shardIndex).count()
  }

  def extract(docID: Int, offset: Int): Any = {
    val shardIndex = locate(docID)
    docValueReaders(shardIndex).setDocument(docID - lower(shardIndex))
    docValueReaders(shardIndex).valueAt(offset)
  }
}

// TODO: All SparkSQL measures will be serialized/deserialized through
// ProjectionAccessor. For now we support VectorUDT
// Sketches (HLL, MinHash, BitMap) can be converted to Array[V] or Map[K, V] and
// pushed through ProjectionAccessor as well
class ProjectionAccessor(luceneReaders: Seq[LuceneReader],
                         fieldName: String)
  extends BinaryAccessor(luceneReaders, fieldName) with SparkSQLProjections {

  override def extract(docID: Int, offset: Int): Any = {
    val bytes = extractBytes(docID, offset)
    unsafeRow.pointTo(bytes, VectorType.sqlType.size, bytes.size)
    VectorType.deserialize(unsafeRow.asInstanceOf[InternalRow])
  }
}

// TODO: To support data types not in SparkSQL but they are part of RDD
// Needed for RDD based API which can have SparkSQL Types + Custom Types
class KryoAccessor[T: ClassTag](luceneReaders: Seq[LuceneReader],
                     fieldName: String,
                     ser: SerializerInstance)
  extends BinaryAccessor(luceneReaders, fieldName) {

  override def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    val bytes = docValueReaders(shardIndex).get(docID - lower(shardIndex)).bytes
    ser.deserialize[T](ByteBuffer.wrap(bytes))
  }
}

object DocValueAccessor extends Serializable {
  def apply(luceneReaders: Seq[LuceneReader],
            fieldName: String,
            dataType: DataType,
            multiValued: Boolean,
            ser: SerializerInstance): DocValueAccessor = {
    // TODO: Test multivalue time stamp column for optimizing document size
    if (multiValued) new ArrayNumericAccessor(luceneReaders, fieldName)
    else {
      dataType match {
        case IntegerType => new NumericAccessor(luceneReaders, fieldName)
        case LongType => new NumericAccessor(luceneReaders, fieldName)
        case FloatType => new NumericAccessor(luceneReaders, fieldName)
        case DoubleType => new NumericAccessor(luceneReaders, fieldName)
        case TimestampType => new NumericAccessor(luceneReaders, fieldName)
        case StringType => new StringAccessor(luceneReaders, fieldName)
        case BinaryType => new BinaryAccessor(luceneReaders, fieldName)
        // Use Kryo if ProjectionAccessor does not work on Vector
        // case _: VectorUDT => new KryoAccessor[Vector](luceneReaders, fieldName, ser)
        case _: VectorUDT => new ProjectionAccessor(luceneReaders, fieldName)
        case _ => throw new LuceneDAOException(s"unsupported type ${dataType} for " +
          s"docvalue accessor constructor")
      }
    }
  }
}
