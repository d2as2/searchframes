package org.apache.spark.util

import org.apache.spark.SparkConf
/**
 * @author debasish83 on 12/15/16.
 */
object DalUtils {
  def getLocalDir(conf: SparkConf) : String = {
    Utils.getLocalDir(conf)
  }
}
