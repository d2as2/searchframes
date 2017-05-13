package org.apache.spark.util

import org.apache.spark.{HashPartitioner, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashMap
import scala.reflect.ClassTag

/**
  * Created by v606014 on 5/10/17.
  */

object RDDUtils extends Logging {
  type SparkOpenHashMap[K,V] = OpenHashMap[K,V]

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

    val cleanSeqOp = in.context.clean(seqOp)
    val cleanCombOp = in.context.clean(combOp)
    val aggregatePartition =
      (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
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
      }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
    }
    // push the final aggregation results to spark executor
    partiallyAggregated.mapPartitionsWithIndex {
      (i, iter) => iter.map((executorId, _))
    }.reduceByKey(new HashPartitioner(1), cleanCombOp).values
  }
}


