package com.verizon.bda.trapezium.dal.lucene

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.sql.Time

class LuceneDAOSuite extends FunSuite with SharedSparkContext with BeforeAndAfterAll {
  val outputPath = "target/luceneIndexerTest/"
  val hdfsIndexPath = new Path(outputPath, "hdfs").toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.registerKryoClasses(Array(classOf[IndexSearcher],
      classOf[DictionaryManager]))
    cleanup()
  }

  override def afterAll(): Unit = {
    cleanup()
    super.afterAll()
  }
  
  private def cleanup(): Unit = {
    val f = new File(outputPath)
    if (f.exists()) {
      FileUtils.deleteQuietly(f)
    }
  }

  test("IndexTest") {
    val dimensions = Set("zip", "tld")

    val types =
      Map("user" -> LuceneType(false, StringType),
          "zip" -> LuceneType(false, StringType),
          "tld" -> LuceneType(true, StringType),
          "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(hdfsIndexPath, dimensions, types)
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
          ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits")

    val indexTime = new Time(System.nanoTime())
    dao.index(df, indexTime)
    dao.load(sc)

    assert(dao.search("tld:google.com").count == 2)
    assert(dao.search("tld:verizon.com").count == 1)
  }
}
