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

  val dimensions = Set("zip", "tld")

  val types =
    Map("user" -> LuceneType(false, StringType),
      "zip" -> LuceneType(false, StringType),
      "tld" -> LuceneType(true, StringType),
      "visits" -> LuceneType(false, IntegerType))

  val dao = new LuceneDAO(hdfsIndexPath, dimensions, types)

  var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = SQLContext.getOrCreate(sc)
    conf.registerKryoClasses(Array(classOf[IndexSearcher],
      classOf[DictionaryManager]))
    //cleanup()
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

  test("DictionaryEncoding") {
    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    val dm = dao.encodeDictionary(df)
    assert(dm.size() == 5)

    val zipRange = dm.getRange("zip")
    val tldRange = dm.getRange("tld")

    val idx1 = dm.indexOf("zip", "94555")
    val idx2 = dm.indexOf("tld", "verizon.com")

    assert(idx1 >= zipRange._1 && idx1 < zipRange._2)
    assert(idx2 >= tldRange._1 && idx2 < tldRange._2)
  }

  test("IndexTest") {
    // With coalesce > 2 partition run and 0 leafReader causes
    // maxHits = 0 on which an assertion is thrown
    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    val indexTime = new Time(System.nanoTime())
    dao.index(df, indexTime)

    dao.load(sc)

    val rdd1 = dao.search("tld:google.com")
    val rdd2 = dao.search("tld:verizon.com")

    assert(rdd1.count == 2)
    assert(rdd2.count == 1)

    assert(rdd1.map(_.getAs[String](0)).collect.toSet == Set("123", "456"))
    assert(rdd2.map(_.getAs[String](0)).collect.toSet == Set("123"))
  }
}
