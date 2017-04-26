package com.verizon.bda.trapezium.dal.lucene

import scala.collection.mutable.HashSet
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus.Builder

/**
 * @author debasish83 on 12/24/16.
 *         aggregator functions for lucene dao
 */

// TODO: OLAPAggregator should extend SparkSQL AggregateFunction and
// Use ImperativeAggregate / DeclarativeAggregate
// TODO: StatFunction in SparkSQL is using ML style aggregate pattern toRDD.aggregate
// but not using treeAggregate, consult mailing list
abstract class OLAPAggregator extends Serializable {
  // mutableAggBuffer is of size dim1 x dim2 x dim3, It's value is based on measure ?
  // def initialize(mutableAggBuffer: InternalRow)

  def init(size: Int): Unit

  def update(idx: Int, input: Any): Unit

  // It is possible to move idx: Int, input: Any as inputRow: InternalRow
  // def update(mutableAggBuffer: InternalRow, inputRow: InternalRow(idx: Int, input: Any)
  // def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit

  def merge(other: OLAPAggregator): OLAPAggregator

  def get(idx: Int): Any

  def eval(): Array[Any]
}

class Sum extends OLAPAggregator {
  var buffer: Array[Float] = _

  override def init(size: Int): Unit = {
    buffer = Array.fill[Float](size)(0)
  }

  override def update(idx: Int, input: Any): Unit = {
    buffer(idx) += input.asInstanceOf[Long].toFloat
  }

  def merge(other: OLAPAggregator): OLAPAggregator = {
    var idx = 0
    while (idx < buffer.length) {
      buffer(idx) += other.get(idx).asInstanceOf[Float]
      idx += 1
    }
    this
  }

  def get(idx: Int): Any = buffer(idx)

  def eval(): Array[Any] = buffer.map(_.asInstanceOf[Any])
}

// TODO: Cardinality should be calculated using BitMap
class Cardinality extends OLAPAggregator {
  var counter: Array[HashSet[Any]] = _

  override def init(size: Int): Unit = {
    counter = Array.fill[HashSet[Any]](size)(new HashSet[Any])
  }

  override def update(idx: Int, input: Any): Unit = {
    counter(idx).add(input)
  }

  def merge(other: OLAPAggregator): Cardinality = {
    var idx = 0
    while (idx < counter.size) {
      other.get(idx).asInstanceOf[HashSet[Any]].foreach(elem => {
        counter(idx).add(elem)
      })
      idx += 1
    }
    this
  }

  def get(idx: Int): Any = counter(idx)

  def eval(): Array[Any] = counter.map(_.size)
}

class CardinalityEstimator(rsd : Double = 0.05) extends OLAPAggregator {

  val p = CardinalityEstimator.accuracy(rsd)

  var buffer: Array[HyperLogLogPlus] = _

  def init(size: Int): Unit = {
    buffer = Array.fill[HyperLogLogPlus](size)(new HyperLogLogPlus(p))
  }

  def update(idx: Int, input: Any): Unit = {
    buffer(idx).offer(input)
  }

  def get(idx: Int): Any = buffer(idx)

  def merge(other: OLAPAggregator): OLAPAggregator = {
    var idx = 0
    while (idx < buffer.length) {
      buffer(idx).addAll(other.get(idx).asInstanceOf[HyperLogLogPlus])
      idx += 1
    }
    this
  }

  def eval(): Array[Any] = buffer.map(_.cardinality())
}

// TODO: Add BitMap and MinHash sketches
class SketchAggregator(rsd: Double = 0.05) extends CardinalityEstimator(rsd) {
  // input is a sketch of type Array[Byte], need to construct/re-use
  // a previous sketch

  override def update(idx: Int, input: Any): Unit = {
    val hll = Builder.build(input.asInstanceOf[Array[Byte]])
    buffer(idx).addAll(hll)
  }
}

object CardinalityEstimator {
  def accuracy(rsd: Double): Int = {
    Math.ceil(2.0d * Math.log(1.106d / rsd) / Math.log(2.0d)).toInt
  }
}