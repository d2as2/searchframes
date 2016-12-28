package com.verizon.bda.trapezium.dal.lucene

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.sql.Time
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.SparseVector

class LuceneDAOSuite extends FunSuite with SharedSparkContext with BeforeAndAfterAll {
  val outputPath = "target/luceneIndexerTest/"
  val indexTime = new Time(System.nanoTime())

  var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = SQLContext.getOrCreate(sc)
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

  test("DictionaryEncoding") {
    val dimensions = Set("zip", "tld")

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dictPath = new Path(outputPath, "hdfs").toString
    val dao = new LuceneDAO(dictPath, dimensions, types)

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

  test("index test") {
    val dimensions = Set("zip", "tld")

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val indexPath = new Path(outputPath, "hdfs").toString
    val dao = new LuceneDAO(indexPath, dimensions, types)

    // With coalesce > 2 partition run and 0 leafReader causes
    // maxHits = 0 on which an assertion is thrown
    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    dao.index(df, indexTime)

    dao.load(sc)

    val rdd1 = dao.search("tld:google.com")
    val rdd2 = dao.search("tld:verizon.com")

    assert(rdd1.count == 2)
    assert(rdd2.count == 1)

    assert(rdd1.map(_.getAs[String](0)).collect.toSet == Set("123", "456"))
    assert(rdd2.map(_.getAs[String](0)).collect.toSet == Set("123"))
  }

  test("vector test") {
    val indexPath = new Path(outputPath, "vectors").toString

    val dimensions = Set("zip")

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "visits" -> LuceneType(false, new VectorUDT()))

    val sv = Vectors.sparse(2, Array(2, 4), Array(5.0, 8.0))
    val user1 = ("123", "94555", sv)
    val user2 = ("456", "94310", Vectors.sparse(3, Array(1, 3, 5), Array(4.0, 7.0, 9.0)))
    val df2 = sqlContext.createDataFrame(
      Seq(user1, user2))
      .toDF("user", "zip", "visits").coalesce(2)

    val dao = new LuceneDAO(indexPath, dimensions, types)
    dao.index(df2, indexTime)
    dao.load(sc)

    val row = dao.search("zip:94555").collect()(0)

    assert(row.getAs[String](0) == "123")
    assert(row.getAs[SparseVector](2) == sv)
  }

  test("numeric sum test") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "numeric").toString

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    val dao = new LuceneDAO(indexPath, dimensions, types)
    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "visits")
    assert(result.length == 2)

    val result2 = dao.group("tld:verizon.com", "zip", "visits")
    assert(result2(0) == 8)
  }

  test("cardinality estimator test") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "cardinality").toString

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12),
        ("314", "94555", Array("google.com", "amazon.com"), 10)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)
    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user")
    assert(result.length == 2)
    val result2 = dao.group("tld:amazon.com", "zip", "user")
    println(result2.mkString(","))
    assert(result2(0) == 1)
    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user")
    println(result3.mkString(","))
    assert(result3(0) == 2)
  }
}
