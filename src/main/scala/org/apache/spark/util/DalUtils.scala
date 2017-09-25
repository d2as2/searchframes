package org.apache.spark.util

import java.io.File
import java.util.UUID
import org.apache.spark.SparkConf
/**
 * @author debasish83 on 12/15/16.
 */
object DalUtils {
  def getLocalDir(conf: SparkConf) : String = {
    Utils.getLocalDir(conf)
  }

  def getTempFile(identifier: String, path: File): File = {
    new File(path.getAbsolutePath + s".${identifier}." + UUID.randomUUID())
  }
}
