package com.verizon.bda.trapezium.dal.lucene

import java.io.IOException
import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, PathFilter, Path => HadoopPath}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import java.sql.Time
import java.io.File
import org.apache.spark.util.{DalUtils, RDDUtils}
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.mllib.linalg.Vectors

class LuceneDAO(val location: String,
                val searchFields: Set[String],
                val searchAndStoredFields: Set[String],
                val storedFields: Set[String],
                storageLevel: StorageLevel = StorageLevel.DISK_ONLY) extends Serializable {
  @transient lazy val log = Logger.getLogger(classOf[LuceneDAO])
  @transient private lazy val analyzer = new KeywordAnalyzer

  val PREFIX = "trapezium-lucenedao"
  val SUFFIX = "part-"

  val INDEX_PREFIX = "index"
  val DICTIONARY_PREFIX = "dictionary"
  val SCHEMA_PREFIX = "schema"

  val converter = OLAPConverter(searchFields, searchAndStoredFields, storedFields)
  private var _dictionary: DictionaryManager = _

  def encodeDictionary(df: DataFrame): LuceneDAO = {
    if (_dictionary != null) return this
    _dictionary = new DictionaryManager
    searchAndStoredFields.foreach(f => {
      val selectedDim =
        if (df.schema(f).dataType.isInstanceOf[ArrayType]) {
          df.select(explode(df(f)))
        } else {
          df.select(df(f))
        }
      val filteredDim = selectedDim.rdd.map(_.getAs[String](0)).filter(_ != null)
      _dictionary.addToDictionary(f, filteredDim)
    })
    this
  }

  /**
    * Udf to compute the indices and values for sparse vector.
    * Here s is storedDimension and m is featureColumns mapped as Array
    * corresponding to the dimensions. Support measures that are MapType
    * with keys as the hierarchical dimension
    */
  val featureIndexUdf = udf { (s: mutable.WrappedArray[String],
                               m: mutable.WrappedArray[Map[String, Double]]) =>
    val indVal = s.zip(m).flatMap { x =>
      if (x._2 == null)
        Map[Int, Double]()
      else {
        val output: Map[Int, Double] = x._2.map(kv => (_dictionary.indexOf(x._1, kv._1), kv._2))
        output.filter(_._1 >= 0)
      }
    }.sortBy(_._1)
    Vectors.sparse(_dictionary.size, indVal.map(_._1).toArray, indVal.map(_._2).toArray)
  }

  /**
    * 2 step process to generate feature vectors:
    * Step 1: Collect all the featureColumns in arrFeatures that
    * should go into feature vector.
    * Step 2: Pass arrFeatures and the featureColumns to featureIndexUdf
    * to generate the vector
    */
  def vectorize(df: DataFrame, vectorizedColumn: String,
                dimension: Seq[String],
                features: Seq[String]): DataFrame = {
    val dfWithFeatures = df.withColumn("arrFeatures", array(features.map(df(_)): _*))
    val vectorized = dfWithFeatures.withColumn(vectorizedColumn,
      featureIndexUdf(array(dimension.map(lit(_)): _*), dfWithFeatures("arrFeatures")))
    vectorized.drop("arrFeatures")
  }

  // TODO: If index already exist we have to merge dictionary and update indices
  def index(dataframe: DataFrame, time: Time): Unit = {
    val locationPath = location.stripSuffix("/") + "/"
    val indexPath = locationPath + INDEX_PREFIX
    val dictionaryPath = locationPath + DICTIONARY_PREFIX
    val schemaPath = locationPath + SCHEMA_PREFIX

    val path = new HadoopPath(locationPath)
    val conf = new Configuration
    val fs = FileSystem.get(path.toUri, conf)
    if (fs.exists(path)) {
      log.warn(s"deleting indices at location ${path.getName}")
      fs.delete(path, true)
    }
    fs.mkdirs(path)
    encodeDictionary(dataframe)

    val sc: SparkContext = dataframe.rdd.sparkContext
    val dictionaryBr = dataframe.rdd.context.broadcast(dictionary)
    val parallelism = dataframe.rdd.context.defaultParallelism
    val inSchema = dataframe.schema

    dataframe.coalesce(parallelism).rdd.mapPartitionsWithIndex((i, itr) => {
      val sparkConf = new SparkConf()
      val localDir = new File(DalUtils.getLocalDir(sparkConf))
      log.info(s"Created ${localDir} to write lucene shuffle")

      val indexWriterConfig = new IndexWriterConfig(analyzer)
      indexWriterConfig.setOpenMode(OpenMode.CREATE)
      // Open a directory on Standalone/YARN/Mesos disk cache
      val shuffleIndexFile = DalUtils.getTempFile(PREFIX, localDir)
      val shuffleIndexPath = shuffleIndexFile.toPath
      log.info(s"Created ${shuffleIndexPath} to write lucene partition $i shuffle")
      converter.setSchema(inSchema)
      converter.setDictionary(dictionaryBr.value)
      log.info(s"codec used for index creation ${indexWriterConfig.getCodec.getName}")

      val directory = new MMapDirectory(shuffleIndexPath)
      val indexWriter = new IndexWriter(directory, indexWriterConfig)

      itr.foreach {
        r => {
          try {
            val d = converter.rowToDoc(r)
            indexWriter.addDocument(d);
          } catch {
            case e: Throwable => {
              throw new LuceneDAOException(s"Error with adding row ${r} " +
                s"to document ${e.getStackTraceString}")
            }
          }
        }
      }
      indexWriter.commit()
      log.debug("Number of documents indexed in this partition: " + indexWriter.maxDoc())
      indexWriter.close
      val conf = new Configuration
      val partitionLocation = indexPath + "/" + SUFFIX + i

      val dstPath = new HadoopPath(partitionLocation)
      val srcPath = new HadoopPath(shuffleIndexPath.toString)

      FileSystem.get(conf).copyFromLocalFile(true, srcPath, dstPath)
      log.info(s"Copied indices from ${srcPath.toString} to deep storage ${dstPath.toString}")

      if (shuffleIndexFile.exists() && !shuffleIndexFile.delete()) {
        log.error(s"Error while deleting temp file ${shuffleIndexFile.getAbsolutePath()}")
      }
      Iterator.empty
    }).count()

    val filesList = fs.listFiles(path, true)
    while (filesList.hasNext())
      log.debug(filesList.next().getPath.toString())

    FileSystem.closeAll()

    dictionary.save(dictionaryPath)(sc)
    // save the schema object
    sc.parallelize(dataframe.schema, 1).saveAsObjectFile(schemaPath)

    log.info("Number of partitions: " + dataframe.rdd.getNumPartitions)
  }

  // TODO: load logic will move to LuceneRDD
  @transient private var _shards: RDD[LuceneShard] = _

  def load(sc: SparkContext): Unit = {
    val indexPath = location.stripSuffix("/") + "/" + INDEX_PREFIX
    val dictionaryPath = location.stripSuffix("/") + "/" + DICTIONARY_PREFIX
    val schemaPath = location.stripSuffix("/") + "/" + SCHEMA_PREFIX

    // load the schema object into RDD
    val schemaRDD = sc.objectFile(schemaPath).map{x: Any => x.asInstanceOf[StructField]}
    val schema = StructType(schemaRDD.collect)

    converter.setSchema(schema)

    val indexDir = new HadoopPath(indexPath)
    val fs = FileSystem.get(indexDir.toUri, sc.hadoopConfiguration)
    val status: Array[FileStatus] = fs.listStatus(indexDir, new PathFilter {
      override def accept(path: HadoopPath): Boolean = {
        path.getName.startsWith(SUFFIX)
      }
    })
    val numPartitions = status.length
    log.info(s"Loading ${numPartitions} indices from path ${indexPath}")

    val partitionIds = sc.parallelize((0 until numPartitions).toList, sc.defaultParallelism)

    _shards = RDDUtils.mapPartitionsInternal(partitionIds, (indices: Iterator[Int]) => {
      indices.map((index: Int) => {
        val sparkConf = new SparkConf()
        val localDir = new File(DalUtils.getLocalDir(sparkConf))
        log.info(s"Created ${localDir} to write lucene shuffle")

        val hdfsPath = indexPath + "/" + SUFFIX + index
        // Open a directory on Standalone/YARN/Mesos disk cache
        val shuffleIndexFile = DalUtils.getTempFile(PREFIX, localDir)
        val shuffleIndexPath = shuffleIndexFile.toPath
        log.info(s"Created ${shuffleIndexPath} to read lucene partition $index shuffle")

        val conf = new Configuration
        val shard: Option[LuceneShard] = {
          log.info(s"Copying data from deep storage: ${hdfsPath} to " +
            s"local shuffle: ${shuffleIndexPath}")
          try {
            FileSystem.get(conf).copyToLocalFile(false,
              new HadoopPath(hdfsPath),
              new HadoopPath(shuffleIndexPath.toString))
            val directory = new MMapDirectory(shuffleIndexPath)
            val reader = DirectoryReader.open(directory)
            Some(LuceneShard(reader, converter))
          } catch {
            case e: IOException =>
              throw new LuceneDAOException(s"Copy from: ${hdfsPath} to local " +
                s"shuffle: ${shuffleIndexPath} failed")
            case x: Throwable => throw new RuntimeException(x)
          }
        }
        shard.get
      })
    })
    _shards.cache()

    log.info("Number of shards: " + shards.count())
    if (_dictionary == null) _dictionary = new DictionaryManager
    else _dictionary.clear()

    _dictionary.load(dictionaryPath)(sc)
    log.info(s"dictionary stats ${_dictionary}")
  }

  def shards(): RDD[LuceneShard] = _shards

  def dictionary(): DictionaryManager = _dictionary

  def count(queryStr: String): Double = {
    if (shards == null) throw new LuceneDAOException(s"count called with null shards")
    shards.map(_.searchDocs(queryStr).cardinality()).sum()
  }

  def search(queryStr: String,
             columns: Seq[String],
             sample: Double): RDD[Row] = {
    if (shards == null) throw new LuceneDAOException(s"search called with null shards")
    val rows = shards.flatMap((shard: LuceneShard) => {
      converter.setColumns(columns)
      shard.search(queryStr, columns, sample)
    })
    rows
  }

  //search a query and retrieve for all stored fields
  def search(queryStr: String, sample: Double = 1.0) : RDD[Row] = {
    search(queryStr, storedFields.toSeq ++ searchAndStoredFields.toSeq, sample)
  }

  private val aggFunctions = Set("sum", "count_approx", "count", "sketch")

  // TODO: Aggregator will be instantiated based on the operator and measure
  // Eventually they will extend Expression from Catalyst but run columnar processing
  private def getAggregator(aggFunc: String): OLAPAggregator = {
    if (aggFunctions.contains(aggFunc)) {
      aggFunc match {
        case "sum" => new Sum
        case "count_approx" => new CardinalityEstimator
        case "count" => new Cardinality
        case "sketch" => new SketchAggregator
      }
    } else {
      throw new LuceneDAOException(s"unsupported aggFunc $aggFunc " +
        s"supported ${aggFunctions.mkString(",")}")
    }
  }

  // TODO: Look into treeAggregate architecture for multiple queries
  private def combOp = (agg: OLAPAggregator,
                        other: OLAPAggregator) => {
    agg.merge(other)
  }
  
  // TODO: Multiple  measures can be aggregated at same time
  def aggregate(queryStr: String,
                measure: String,
                aggFunc: String): Any = {
    if (shards == null) throw new LuceneDAOException(s"aggregate called with null shards")

    log.info(s"query ${queryStr} measure ${measure}, aggFunc $aggFunc")

    val seqOp = (agg: OLAPAggregator,
                 shard: LuceneShard) => {
      shard.aggregate(
        queryStr,
        measure,
        agg)
    }
    val agg = getAggregator(aggFunc)
    val aggStart = System.nanoTime()
    agg.init(1)
    val results = shards.treeAggregate(agg)(seqOp, combOp)
    println(f"OLAP aggragation time ${(System.nanoTime() - aggStart) * 1e-9}%6.3f sec")
    results.eval()(0)
  }

  // TODO: time-series and group should be combined in group, bucket boundaries on any measure
  // is feasible to generate like time-series
  def group(queryStr: String,
            dimension: String,
            measure: String,
            aggFunc: String): Map[String, Any] = {
    if (shards == null) throw new LuceneDAOException(s"group called with null shards")

    val dimRange = dictionary.getRange(dimension)
    val dimOffset = dimRange._1
    val dimSize = dimRange._2 - dimRange._1 + 1

    log.info(s"query ${queryStr} dimension ${dimension}, " +
      s"range [${dimRange._1}, ${dimRange._2}] measure ${measure}")

    // TODO: Aggregator is picked based on the SQL functions sum, countDistinct, count
    val agg = getAggregator(aggFunc)

    // TODO: If aggregator is not initialized from driver and broadcasted, merge fails on NPE
    // TODO: RDD aggregate needs to be looked into

    val conf = shards.sparkContext.getConf
    val executorAggregate = conf.get("spark.trapezium.executoraggregate", "false").toBoolean
    println(s"executorAggregate ${executorAggregate}")
    val depth = conf.get("spark.trapezium.aggregationdepth", "2").toInt
    println(s"aggregation depth ${depth}")

    val seqOp = (agg: OLAPAggregator,
                 shard: LuceneShard) => shard.group(
      queryStr,
      dimension,
      dimOffset,
      measure,
      agg = agg)

    agg.init(dimSize)

    val groups = if (executorAggregate) {
      val groupStart = System.nanoTime()
      val partitions = shards.sparkContext.defaultParallelism
      val executorId = Math.floor(Random.nextDouble() * partitions).toInt
      val results =
        RDDUtils.treeAggregateExecutor(agg)(
          shards,
          seqOp,
          (agg, other) => agg.merge(other),
          depth,
          executorId).map(_.eval().zipWithIndex)
      results.count()
      println(f"OLAP group time ${(System.nanoTime() - groupStart) * 1e-9}%6.3f sec")
      results.collect()(0)
    } else {
      val groupStart = System.nanoTime()
      // TODO: optimize on agg broadcast
      agg.init(dimSize)
      val results = shards.treeAggregate(agg)(seqOp, combOp, depth)
      println(f"OLAP group time ${(System.nanoTime() - groupStart) * 1e-9}%6.3f sec")
      results.eval().zipWithIndex
    }

    val transformed = groups.map { case (value: Any, index: Int) =>
      (dictionary.getFeatureName(dimOffset + index), value)
    }.toMap
    transformed
  }

  // TODO: time-series for multiple measure can be aggregated in same call
  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String,
                 aggFunc: String): Array[Any] = {
    if (shards == null) throw new LuceneDAOException(s"timeseries called with null shards")

    log.info(s"query ${queryStr} measure ${measure}, time [$minTime, $maxTime] rollup $rollup")

    val dimSize = Math.floor((maxTime - minTime) / rollup).toInt
    log.info(s"Calculated time series size ${dimSize} " +
      s"from [$maxTime, $minTime] with rollup $rollup")

    val seqOp = (agg: OLAPAggregator,
                 shard: LuceneShard) => {
      shard.timeseries(
        queryStr,
        minTime,
        maxTime,
        rollup,
        measure,
        agg)
    }

    val agg = getAggregator(aggFunc)

    val tsStart = System.nanoTime()
    agg.init(dimSize)
    val results = shards.treeAggregate(agg)(seqOp, combOp)
    println(f"OLAP timeseries time ${(System.nanoTime() - tsStart) * 1e-9}%6.3f sec")
    results.eval
  }

  def facet(queryStr: String,
            dimension: String): Map[String, Long] = {
    if (shards == null) throw new LuceneDAOException(s"timeseries called with null shards")

    log.info(s"query ${queryStr} dimension ${dimension}")
    val agg = getAggregator("sum")

    val dimRange = dictionary.getRange(dimension)
    val dimSize = dimRange._2 - dimRange._1 + 1
    val dimOffset = dimRange._1

    val seqOp = (agg: OLAPAggregator,
                 shard: LuceneShard) => {
      shard.facet(
        queryStr,
        dimension,
        dimOffset,
        agg)
    }

    val facetStart = System.nanoTime()
    agg.init(dimSize)
    val results = shards.treeAggregate(agg)(seqOp, combOp)
    println(f"OLAP facet time ${(System.nanoTime() - facetStart) * 1e-9}%6.3f sec")
    val facets = results.eval().zipWithIndex

    val transformed = facets.map { case (value: Any, index: Int) =>
      (dictionary.getFeatureName(dimOffset + index), value.asInstanceOf[Long])
    }.toMap
    transformed
  }
}
