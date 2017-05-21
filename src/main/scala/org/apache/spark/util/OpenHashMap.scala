package org.apache.spark.util

import org.apache.spark.util.collection.OpenHashMap
import scala.reflect.ClassTag

/**
  * @author debasish83 on 5/21/17.
  *         Utilities to expose various collections from Spark and FastUtil projects
  */
case class SparkOpenHashMap[K: ClassTag, V: ClassTag](capacity: Int)
  extends OpenHashMap[K, V](capacity)
