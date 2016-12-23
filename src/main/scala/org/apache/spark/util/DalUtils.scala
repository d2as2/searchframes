package org.apache.spark.util

import java.io.File
import org.apache.spark.SparkConf
/**
 * @author debasish83 on 12/15/16.
 */
object DalUtils {
  def getLocalDir(conf: SparkConf) : String = {
    Utils.getLocalDir(conf)
  }

  def getTempFile(prefix: String, path: File, suffix: String): File = {
    Utils.tempFileWith(path)
  }
}
