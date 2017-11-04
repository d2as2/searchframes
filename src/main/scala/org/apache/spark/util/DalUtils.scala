package org.apache.spark.util

import java.io.File
import java.util.UUID
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection

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

  def deserializeVector(row: InternalRow): Vector = {
    VectorType.asInstanceOf[VectorUDT].deserialize(row)
  }

  def serializeVector(value: Vector): InternalRow = {
    VectorType.asInstanceOf[VectorUDT].serialize(value)
  }

  def projectVector(): UnsafeProjection = {
    UnsafeProjection.create(VectorType.asInstanceOf[VectorUDT].sqlType)
  }
}
