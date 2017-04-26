package com.verizon.bda.trapezium.dal.lucene

import java.io.File
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.SparseVector
import java.sql.Time
import java.sql.Timestamp
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.sql.types._

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

  test("cardinality estimator with null dimension") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "nulldim").toString

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
    val visits = dao.aggregate("tld:google.com", "visits", "sum")
    assert(visits == 18)
  }

  test("cardinality estimator test with sketch") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "sketch").toString

    val types =
      Map("user" -> LuceneType(false, BinaryType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    val p = CardinalityEstimator.accuracy(0.05)

    val user1 = new HyperLogLogPlus(p)
    user1.offer("123")
    user1.offer("314")
    val user2 = new HyperLogLogPlus(p)
    user2.offer("456")
    user2.offer("512")
    val user3 = new HyperLogLogPlus(p)
    user3.offer("314")
    user3.offer("124")

    val df = sqlContext.createDataFrame(
      Seq((user1.getBytes, "94555", Array("verizon.com", "google.com"), 8),
        (user2.getBytes, "94310", Array("apple.com", "google.com"), 12),
        (user3.getBytes, "94555", Array("google.com", "amazon.com"), 10)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user", "sketch")
    assert(result.size == 2)

    assert(result("94555") == 3)
    assert(result("94310") == 2)

    val result2 = dao.group("tld:amazon.com", "zip", "user", "sketch")
    assert(result2("94555") == 2)
    assert(result2("94310") == 0)

    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user", "sketch")
    assert(result3.size == 2)
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
    assert(result("94555") == 2)
    assert(result("94310") == 1)

    val result2 = dao.group("tld:amazon.com", "zip", "user", "count_approx")
    assert(result2("94555") == 1)
    assert(result2("94310") == 0)

    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user", "count_approx")
    assert(result3.size == 2)
  }

  test("cardinality estimator with null measure") {
    val dimensions = Set("zip", "tld")
    val indexPath = new Path(outputPath, "nullmeasure").toString

    val types =
      Map("user" -> LuceneType(false, StringType),
        "zip" -> LuceneType(false, StringType),
        "tld" -> LuceneType(true, StringType),
        "visits" -> LuceneType(false, IntegerType))

    val dao = new LuceneDAO(indexPath, dimensions, types)

    // TODO: Add dimension, string and byte measure null
    val data = sc.parallelize(
      Seq(Row("123", "94555", Array("verizon.com", "google.com"), 8),
        Row("456", "94310", Array("apple.com", null), 12),
        Row("314", null, Array("google.com", "amazon.com"), null)))

    val schema = StructType(Seq(StructField("user", StringType, true),
      StructField("zip", StringType, true),
      StructField("tld", ArrayType(StringType, true), true),
      StructField("visits", IntegerType, true)))

    val df = sqlContext.createDataFrame(data, schema)

    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user", "count_approx")
    assert(result.size == 2)

    /* TODO: null single valued dimension encoded as 0 due to NumericDocValues
    assert(result("94555") == 1)
    assert(result("94310") == 1)
    */

    val visits = dao.aggregate("tld:google.com", "visits", "sum")
    // TODO : For numeric measures doc values pads 0 as MISSING
    // For other data types further analysis is needed
    assert(visits == 8)
  }

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

  // TODO: Add a local test where multiple-leaf readers are generated by modifying IndexWriterConfig
}
