package com.verizon.bda.trapezium.dal.lucene

import java.io.File
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
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
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("visits")

    val dictPath = new Path(outputPath, "hdfs").toString
    val dao = new LuceneDAO(dictPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    val dm = dao.encodeDictionary(df).dictionary()
    assert(dm.size() == 5)

    val zipRange = dm.getRange("zip")
    val tldRange = dm.getRange("tld")

    val idx1 = dm.indexOf("zip", "94555")
    val idx2 = dm.indexOf("tld", "verizon.com")

    assert(idx1 >= zipRange._1 && idx1 < zipRange._2)
    assert(idx2 >= tldRange._1 && idx2 < tldRange._2)
  }
  //FIXME: Failing on Jenkins
  /*
  test("vectorize test") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld", "appname")
    val measures = Set("user", "svisits", "visits")

    val dictPath = new Path(outputPath, "sparsemeasures").toString

    val dao = new LuceneDAO(dictPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), Array("instagram"),
        Map("verizon.com" -> 1.0, "google.com" -> 4.0), Map("instagram" -> 4.0), 8),
        ("456", "94310", Array("apple.com", "google.com"), null,
          Map("apple.com" -> 8.0, "google.com" -> 3.0), null, 7))).
      toDF("user", "zip", "tld", "appname", "tldvisits", "appvisits", "visits")

    val vectorized = dao.encodeDictionary(df).vectorize(df, "svisits",
      Seq("tld", "appname"), Seq("tldvisits", "appvisits")).drop("tldvisits").drop("appvisits")

    val maxSize = dao.dictionary().size()

    val result1 = vectorized.filter("user = '123'").first().getAs[SparseVector]("svisits")
    assert(result1 == Vectors.sparse(maxSize, Array(2, 4, 5), Array(4.0, 1.0, 4.0)))

    val result2 = vectorized.filter("user = '123'").first().getAs[Seq[String]]("tld")
    assert(result2 == Seq("verizon.com", "google.com"))

    dao.index(vectorized, indexTime)

    dao.load(sc)

    val rdd1 = dao.search("tld:google.com", Seq("user", "svisits"), 1.0)
    assert(rdd1.count() == 2)

    val rdd2 = dao.search("appname:instagram", Seq("user", "svisits"), 1.0)
    assert(rdd2.count() == 1)
    assert(rdd2.collect()(0) == Row.apply("123", Vectors.sparse(maxSize, Array(2, 4, 5), Array(4.0, 1.0, 4.0))))

    val rdd3 = dao.search("appname:instagram", Seq("user", "visits"), 1.0)
    assert(rdd3.count() == 1)
    assert(rdd3.collect()(0) == Row.apply("123", 8))
  }
  */
  
  test("index test") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "hdfs").toString
    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

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

    assert(rdd1.map(_.getAs[String](0).toString).collect.toSet == Set("123", "456"))
    assert(rdd2.map(_.getAs[String](0).toString).collect.toSet == Set("123"))
  }

  ignore("vector test") {
    val indexPath = new Path(outputPath, "vectors").toString

    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip")
    val measures = Set("user", "visits")

    val sv = Vectors.sparse(6, Array(2, 4), Array(5.0, 8.0))
    val user1 = ("123", "94555", sv)
    val user2 = ("456", "94310", Vectors.sparse(6, Array(1, 3, 5), Array(4.0, 7.0, 9.0)))
    val df2 = sqlContext.createDataFrame(
      Seq(user1, user2))
      .toDF("user", "zip", "visits").coalesce(2)

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)
    dao.index(df2, indexTime)
    dao.load(sc)

    val row = dao.search("zip:94555").collect()(0)

    assert(row.getAs[String](0) == "123")
    assert(row.getAs[SparseVector](1) == sv)
  }

  test("numeric sum test") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("visits")

    val indexPath = new Path(outputPath, "numeric").toString

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)
    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "visits", "sum")
    assert(result.size == 2)

    val result2 = dao.group("tld:verizon.com", "zip", "visits", "sum")
    assert(result2("94555") == 8)
    assert(result2("94310") == 0)
  }

  test("cardinality estimator with null dimension") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "nulldim").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

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
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "sketch").toString
    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

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

    val result3 = dao.group("tld:verizon.com OR tld:amazon.com", "zip", "user", "sketch")
    assert(result3.size == 2)
  }

  test("cardinality estimator test") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "cardinality").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

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

  test("search dimension test") {
    val storedDimensions = Set("zip", "tld")
    val dimensions = Set("uri")
    val measures = Set("user", "visits")
    val indexPath = new Path(outputPath, "search").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8, Array("verizon.com-path1", "google.com-path2")),
        ("456", "94310", Array("apple.com", "google.com"), 12, Array("apple.com-path3", "google.com-path2")),
        ("314", "94555", Array("google.com", "amazon.com"), 10, Array("google.com-path2", "amazon.com-path4"))))
      .toDF("user", "zip", "tld", "visits", "uri").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("uri:google.com-path2", "zip", "user", "count_approx")

    assert(result.size == 2)
    assert(result("94555") == 2)
    assert(result("94310") == 1)
  }

  test("count facet on dimension") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")
    val indexPath = new Path(outputPath, "facet").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", "94555", Array("verizon.com", "google.com"), 8),
        ("456", "94310", Array("apple.com", "google.com"), 12),
        ("314", "94555", Array("google.com", "amazon.com"), 10)))
      .toDF("user", "zip", "tld", "visits").coalesce(2)
    dao.index(df, indexTime)
    dao.load(sc)

    val result1 = dao.facet("tld:google.com", "zip")
    assert(result1.size == 2)
    assert(result1("94555") == 2)
    assert(result1("94310") == 1)

    val result2 = dao.facet("tld:google.com", "tld")
    assert(result2.size == 4)
    assert(result2("verizon.com") == 1)
    assert(result2("apple.com") == 1)
    assert(result2("google.com") == 3)
    assert(result2("amazon.com") == 1)
  }

  test("cardinality estimator with null measure") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")
    val indexPath = new Path(outputPath, "nullmeasure").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    // TODO: Add dimension, string and byte measure null
    val data = sc.parallelize(
      Seq(Row("123", "94555", Array("verizon.com", "google.com"), 8),
        Row("456", "94310", Array("apple.com", null), 12),
        Row("314", null, Array("google.com", "amazon.com"), null),
        Row(null, "94310", Array("google.com", "kohls.com"), null)
      ))

    val schema = StructType(Seq(StructField("user", StringType, true),
      StructField("zip", StringType, true),
      StructField("tld", ArrayType(StringType, true), true),
      StructField("visits", IntegerType, true)))

    val df = sqlContext.createDataFrame(data, schema)

    dao.index(df, indexTime)
    dao.load(sc)

    val result = dao.group("tld:google.com", "zip", "user", "count_approx")
    assert(result.size == 2)

    assert(result("94555") == 1)
    assert(result("94310") == 0)

    val visits = dao.aggregate("tld:google.com", "visits", "sum")
    // TODO : For numeric measures doc values pads 0 as MISSING
    // For other data types further analysis is needed
    assert(visits == 8)

    val zipcnt1 = dao.aggregate("tld:google.com", "zip", "count_approx")
    assert(zipcnt1 == 2)

    val zipcnt2 = dao.aggregate("tld:google.com", "user", "count_approx")
    assert(zipcnt2 == 2)
  }

  test("cardinality estimator load test") {
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "cardinality").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)
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
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("user", "visits")

    val indexPath = new Path(outputPath, "cardinality").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)
    dao.load(sc)

    val result = dao.aggregate("tld:google.com", "user", "count")
    assert(result == 3)
  }


  ignore("multivalue dimension test") {
    val indexPath = new Path(outputPath, "multivalue").toString
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("visits", "featureVector")

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", Array("94555", "94301"), Array("verizon.com", "google.com"), "8", Vectors.sparse(10, Array(0, 1, 2, 3), Array(2.0, 4.0, 7.0, 9.0))),
        ("456", Array("95014", "94301"), Array("yahoo.com", "google.com"), "2", Vectors.sparse(10, Array(4, 1, 5, 3), Array(9.0, 8.0, 2.0, 1.0))),
        ("789", Array("94403", "94405"), Array("facebook.com", "att.com"), "3", Vectors.sparse(10, Array(6, 7, 8, 9), Array(1.0, 1.0, 6.0, 3.0)))))
      .toDF("user", "zip", "tld", "visits", "featureVector").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val resultRDD = dao.search("(tld:google.com) AND (zip:95014)")
    assert(resultRDD.collect().size == 1)
    assert(dao.search("*:*").count == 3)
  }

  ignore("multivalue dimension test with null dimensions") {
    val indexPath = new Path(outputPath, "multivalue").toString
    val dimensions = Set.empty[String]
    val storedDimensions = Set("zip", "tld")
    val measures = Set("visits", "featureVector")

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", Array("94555", "94301"), Array("verizon.com", "google.com"), "8", Vectors.sparse(6, Array(0, 1, 2, 3), Array(2.0, 4.0, 7.0, 9.0))),
        ("456", Array("95014", "94301"), Array("yahoo.com", "google.com"), "2", Vectors.sparse(6, Array(4, 1, 5, 3), Array(9.0, 8.0, 2.0, 1.0))),
        ("555", null, null, null, Vectors.sparse(0, Array(), Array()))))
      .toDF("user", "zip", "tld", "visits", "featureVector").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val resultRDD = dao.search("(tld:google.com) AND (zip:95014)")
    assert(resultRDD.collect().size == 1)
    assert(dao.search("*:*").count == 3)
  }


  test("time series test") {
    val dimensions = Set("zip", "tld")
    val storedDimensions = Set("user")
    val measures = Set("time", "visits")
    val indexPath = new Path(outputPath, "time").toString

    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

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

  ignore("null value test") {
    val dimensions = Set("zip", "tld")
    val storedDimensions = Set("user")
    val measures = Set("visits", "featureVector")

    val indexPath = new Path(outputPath, "nullvalue").toString
    val dao = new LuceneDAO(indexPath, dimensions, storedDimensions, measures)

    val df = sqlContext.createDataFrame(
      Seq(("123", null, Array("verizon.com", "google.com"), 8, Vectors.sparse(5, Array(0, 1), Array(2.0, 4.0))),
        ("123", "94301", Array("apple.com", "google.com"), 8, Vectors.sparse(5, Array(1, 3, 4), Array(2.0, 4.0, 6.0))),
        ("456", "94310", Array.empty[String], 12, Vectors.sparse(5, Array(2), Array(9.0))),
        ("314", null, null, 0, Vectors.sparse(5, Array(), Array()))))
      .toDF("user", "zip", "tld", "visits", "featureVector").coalesce(2)

    dao.index(df, indexTime)
    dao.load(sc)

    val result1 = dao.search("(tld:google.com) OR (zip:94310)")
    assert(result1.collect().size == 3)
    val result2 = dao.search("(tld:google.com) AND (zip:94310)")
    assert(result2.collect().size == 0)
    assert(dao.search("*:*").count == 4)

    val resultCount1 = dao.aggregate("tld:google.com", "user", "count")
    assert(resultCount1 == 1)
  }
  //TODO: Add a local test where multiple-leaf readers are generated by modifying IndexWriterConfig
}
