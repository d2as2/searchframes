package org.apache.spark.util

import org.apache.spark.{HashPartitioner}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * @author debasish83 on 5/10/17.
  *         RDD Utilities to expose private spark classes
  */

object RDDUtils  {

  private val log = LoggerFactory.getLogger(this.getClass)
  /**
    * Aggregates the elements of this RDD in a multi-level tree pattern and reduces the
    * final results to an executor identified by the executorId
    *
    * @param executorId
    * @tparam U
    */
  def treeAggregateExecutor[T: ClassTag, U: ClassTag](zeroValue: U)(
    in: RDD[T],
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2,
    executorId: Int): RDD[U] = in.withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val aggregatePartition =
      (it: Iterator[T]) => it.aggregate(zeroValue)(seqOp, combOp)
    var partiallyAggregated = in.mapPartitions(it => Iterator(aggregatePartition(it)))
    var numPartitions = partiallyAggregated.partitions.length
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce
    // the wall-clock time, we stop tree aggregation.
    log.info(s"initial partitions ${numPartitions} scale ${scale} depth ${depth}")
    // Don't trigger TreeAggregation when it doesn't save wall-clock time
    while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
      numPartitions /= scale
      val curNumPartitions = numPartitions
      log.info(s"scaling partitions ${numPartitions}")
      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
        (i, iter) => iter.map((i % curNumPartitions, _))
      }.reduceByKey(new HashPartitioner(curNumPartitions), combOp).values
    }
    // key is within 0 - curNumPartitions
    // push the final aggregation results to spark executor post map side aggregation
    partiallyAggregated.mapPartitionsWithIndex {
      (i, iter) => iter.map((executorId, _))
    }.aggregateByKey(
      zeroValue,
      new HashPartitioner(executorId))(combOp, combOp).values
  }

  /**
   * map partitions without cleaning up the closures
   * @tparam T RDD type
   * @tparam U map partition type
   * @param in RDD
   */
  def mapPartitionsInternal[T: ClassTag, U: ClassTag](
    in: RDD[T],
    f: (Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = {
    in.mapPartitionsInternal(f, preservesPartitioning)
  }
}


