package com.verizon.bda.trapezium.dal.lucene

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.sql.catalyst.expressions.MutableRow

/**
 * @author debasish83 on 12/24/16.
 *         aggregator functions for lucene dao
 */

//TODO: Add an eval that calculates the final value from the aggregator
//TODO: Look into TypedImperativeAggregate and whether we can extend it for LuceneDAO
trait LuceneAggregator extends Serializable {
  def init(size: Int): Unit

  def update(idx: Int, input: Any): Unit

  def merge(other: LuceneAggregator): LuceneAggregator

  def get(idx: Int): Any
}

class NumericSum extends LuceneAggregator {

  var buffer: Array[Float] = _

  override def init(size: Int): Unit = {
    buffer = Array.fill[Float](size)(0)
  }

  override def update(idx: Int, input: Any): Unit = {
    buffer(idx) += input.asInstanceOf[Long].toFloat
  }

  def merge(other: LuceneAggregator): LuceneAggregator = {
    var idx = 0
    while (idx < buffer.length) {
      buffer(idx) += other.get(idx).asInstanceOf[Float]
      idx += 1
    }
    this
  }

  def get(idx: Int) = buffer(idx)
}

class CardinalityEstimator extends LuceneAggregator {

  var buffer: Array[HyperLogLogPlus] = _

  def init(size: Int): Unit = {
    buffer = Array.fill[HyperLogLogPlus](size)(new HyperLogLogPlus(4, 0))
  }

  def update(idx: Int, input: Any): Unit = {
    buffer(idx).offer(input)
  }

  def get(idx: Int) = buffer(idx)

  def merge(other: LuceneAggregator): LuceneAggregator = {
    var idx = 0
    while (idx < buffer.length) {
      buffer(idx).addAll(other.get(idx).asInstanceOf[HyperLogLogPlus])
      idx += 1
    }
    this
  }
}
