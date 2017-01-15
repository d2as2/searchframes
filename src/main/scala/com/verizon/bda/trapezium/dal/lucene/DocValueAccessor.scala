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

trait DocValueLocator extends Serializable {
  val leafBoundaries: Seq[FeatureAttr]

  def locate(docID: Int): Int = {
    var i = 0
    while (i < leafBoundaries.length) {
      if (leafBoundaries(i).contains(docID)) return i
      i += 1
    }
    throw new LuceneDAOException(s"docID $docID not within leafBoundaries ${leafBoundaries.mkString(",")}")
  }

  def lower(index: Int): Int = {
    leafBoundaries(index).dictionaryPos
  }

  def upper(index: Int): Int = {
    lower(index) + leafBoundaries(index).featureOffset + 1
  }
}

abstract class DocValueAccessor(luceneReaders: Seq[LuceneReader],
                                fieldName: String) extends DocValueLocator {
  val leafBoundaries: Seq[FeatureAttr] = luceneReaders.map(_.range)

  val leafReaders: Seq[LeafReader] = luceneReaders.map(_.leafReader)

  def extract(docID: Int, offset: Int): Any

  //doc value offset for a given accessor, default offset is 1
  def getOffset(docID: Int): Int

  //local per partition encoding
  //def size(): size of the doc value vector
  //def distinct(): distinct elements in doc value vector
  //def encode(localIndex: Int): Int generate global index from local index
}

//NumericAccessor work on Long even if incoming types are Int, Float, Long, Double
class NumericAccessor(luceneReaders: Seq[LuceneReader],
                      fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {

  val docValueReaders = leafReaders.map(DocValues.getNumeric(_, fieldName))

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"numeric docvalue accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    docValueReaders(shardIndex).get(docID - lower(shardIndex) + offset)
  }
}

class StringAccessor(luceneReaders: Seq[LuceneReader],
                     fieldName: String,
                     ser: SerializerInstance)
  extends DocValueAccessor(luceneReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSorted(_, fieldName))

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"string docvalue accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    val bytes = docValueReaders(shardIndex).get(docID - lower(shardIndex)).bytes
    //For native types don't use Kryo
    new String(bytes, "UTF-8")
  }
}

class BinaryAccessor[T: ClassTag](luceneReaders: Seq[LuceneReader],
                     fieldName: String,
                     dataType: DataType,
                     ser: SerializerInstance)
  extends DocValueAccessor(luceneReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getBinary(_, fieldName))

  def getOffset(docID: Int): Int = 1

  def extract(docID: Int, offset: Int): Any = {
    assert(offset == 0, s"binary accessor non-zero offset $offset")
    val shardIndex = locate(docID)
    val bytes = docValueReaders(shardIndex).get(docID - lower(shardIndex)).bytes
    ser.deserialize[T](ByteBuffer.wrap(bytes))
  }
}

class ArrayNumericAccessor(luceneReaders: Seq[LuceneReader],
                           fieldName: String)
  extends DocValueAccessor(luceneReaders, fieldName) {
  val docValueReaders = leafReaders.map(DocValues.getSortedNumeric(_, fieldName))

  def getOffset(docID: Int): Int = {
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

object DocValueAccessor extends Serializable {
  def apply(luceneReaders: Seq[LuceneReader],
            fieldName: String,
            dataType: DataType,
            multiValued: Boolean,
            ser: SerializerInstance): DocValueAccessor = {
    //TODO: Test multivalue time stamp column for optimizing document size
    if (multiValued) new ArrayNumericAccessor(luceneReaders, fieldName)
    else {
      dataType match {
        case i: IntegerType => new NumericAccessor(luceneReaders, fieldName)
        case l: LongType => new NumericAccessor(luceneReaders, fieldName)
        case f: FloatType => new NumericAccessor(luceneReaders, fieldName)
        case d: DoubleType => new NumericAccessor(luceneReaders, fieldName)
        case ts: TimestampType => new NumericAccessor(luceneReaders, fieldName)
        case st: StringType => new StringAccessor(luceneReaders, fieldName, ser)
        case v: VectorUDT => new BinaryAccessor[Vector](luceneReaders, fieldName, dataType, ser)
        case _ => throw new LuceneDAOException(s"unsupported type ${dataType} for docvalue accessor constructor")
      }
    }
  }
}
