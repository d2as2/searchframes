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
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.SparseVector
import java.sql.Time
import java.sql.Timestamp

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

    val result = dao.group("tld:google.com", "zip", "visits", "sum")
    assert(result.size == 2)

    val result2 = dao.group("tld:verizon.com", "zip", "visits", "sum")
    assert(result2("94555") == 8)
    assert(result2("94310") == 0)
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

    val result = dao.group("tld:google.com", "zip", "user", "count_approx")
    assert(result.size == 2)

    val result2 = dao.group("tld:amazon.com", "zip", "user", "count_approx")
    assert(result2("94555") == 1)
    assert(result2("94310") == 0)

    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user", "count_approx")
    assert(result3.size == 2)
  }

  /*
  test("cardinality estimator with null dimension") {
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
        ("456", "94310", Array("apple.com", null), 12),
        ("314", null, Array("google.com", "amazon.com"), 10)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)
    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user", "count_approx")
    assert(result.size == 2)
  }
  */

  test("cardinality estimator load test") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "cardinality").toString

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user", "count_approx")
    assert(result.size == 2)

    val result2 = dao.group("tld:amazon.com", "zip", "user", "count_approx")
    assert(result2("94555") == 1)
    assert(result2("94310") == 0)

    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user", "count_approx")
    assert(result3.size == 2)
  }

  test("count test") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "cardinality").toString

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)
    dao.load(sc)

    val result = dao.aggregate("tld:google.com", "user", "count")
    assert(result == 3)
  }


  test("multivalue dimension test") {
    val indexPath = new Path(outputPath, "multivalue").toString
    val dimensions = Set("zip", "tld")
    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(true, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, StringType),
        "featureVector" -> LuceneType(false, new VectorUDT()))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    val df = sqlContext.createDataFrame(
      Seq(("123", Array("94555", "94301"), Array("verizon.com", "google.com"), "8", Vectors.sparse(4, Array(0, 1, 2, 3), Array(2.0, 4.0, 7.0, 9.0))),
        ("456", Array("95014", "94301"), Array("yahoo.com", "google.com"), "2", Vectors.sparse(4, Array(4, 1, 5, 3), Array(9.0, 8.0, 2.0, 1.0))),
        ("789", Array("94403", "94405"), Array("facebook.com", "att.com"), "3", Vectors.sparse(4, Array(6, 7, 8, 9), Array(1.0, 1.0, 6.0, 3.0)))))
      .toDF("user", "zip", "tld", "visits", "featureVector").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val resultRDD = dao.search("(tld:google.com) AND (zip:95014)")
    assert(resultRDD.collect().size == 1)
    assert(dao.search("*:*").count == 3)
  }

  test("multivalue dimension test with null dimensions") {
    val indexPath = new Path(outputPath, "multivalue").toString
    val dimensions = Set("zip", "tld")
    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(true, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, StringType),
        "featureVector" -> LuceneType(false, new VectorUDT()))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    val df = sqlContext.createDataFrame(
      Seq(("123", Array("94555", "94301"), Array("verizon.com", "google.com"), "8", Vectors.sparse(4, Array(0, 1, 2, 3), Array(2.0, 4.0, 7.0, 9.0))),
        ("456", Array("95014", "94301"), Array("yahoo.com", "google.com"), "2", Vectors.sparse(4, Array(4, 1, 5, 3), Array(9.0, 8.0, 2.0, 1.0))),
        ("555", null, null, "", Vectors.sparse(0, Array(), Array()))))
      .toDF("user", "zip", "tld", "visits", "featureVector").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val resultRDD = dao.search("(tld:google.com) AND (zip:95014)")
    assert(resultRDD.collect().size == 1)
    assert(dao.search("*:*").count == 3)
  }


  test("time series test") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "time").toString

    val types =
      Map("time" -> LuceneType(false, TimestampType),
        "user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    val time1 = new Timestamp(100)
    val time2 = new Timestamp(200)
    val time3 = new Timestamp(300)

    val df = sqlContext.createDataFrame(
      Seq((time1, "123", "94555", Array("verizon.com", "google.com"), 8),
        (time1, "456", "94310", Array("apple.com", "google.com"), 12),
        (time1, "123", "94555", Array("google.com", "amazon.com"), 6),
        (time2, "123", "94555", Array("verizon.com", "google.com"), 4),
        (time2, "456", "94310", Array("apple.com", "google.com"), 4),
        (time2, "314", "94555", Array("google.com", "amazon.com"), 10),
        (time3, "456", "94555", Array("verizon.com", "google.com"), 8),
        (time3, "456", "94310", Array("apple.com", "google.com"), 6),
        (time3, "456", "94555", Array("google.com", "verizon.com"), 10)))
      .toDF("time", "user", "zip", "tld", "visits").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val result =
      dao.timeseries(
        queryStr = "tld:google.com",
        minTime = 100,
        maxTime = 400,
        rollup = 100,
        measure = "user",
        aggFunc = "count_approx")

    /* Expected result
    time1: 2 [123:2, 456:1]
    time2: 3 [123:1, 456:1, 314:1]
    time3: 1 [456:3]
    */
    assert(result.length == 3)
    assert(result === Array(2, 3, 1))

    val result2 =
      dao.timeseries(
        queryStr = "tld:verizon.com",
        minTime = 100,
        maxTime = 400,
        rollup = 100,
        measure = "visits",
        aggFunc = "sum")

    assert(result2.length == 3)
    /* Expected result
      time1: 8, time2: 4, time3: 18
     */
    assert(result2 === Array(8, 4, 18))
  }

  //TODO: Add a local test where multiple-leaf readers are generated by modifying IndexWriterConfig

  //TODO: Add a local test with null as values
}
