package com.verizon.bda.trapezium.dal.lucene

import java.io.IOException
import com.verizon.bda.trapezium.dal.exceptions.LuceneDAOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath, PathFilter, FileStatus, FileSystem}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import java.sql.Time
import java.io.File
import org.apache.spark.util.DalUtils
import org.apache.spark.sql.types._

class LuceneDAO(val location: String,
                val dimensions: Set[String],
                val types: Map[String, LuceneType],
                storageLevel: StorageLevel = StorageLevel.DISK_ONLY) extends Serializable {
  @transient lazy val log = Logger.getLogger(classOf[LuceneDAO])

  lazy val analyzer = new KeywordAnalyzer

  val PREFIX = "trapezium-lucenedao"
  val SUFFIX = "part-"

  val INDEX_PREFIX = "index"
  val DICTIONARY_PREFIX = "dictionary"

  val converter = OLAPConverter(dimensions, types)

  private var dictionary: DictionaryManager = _

  def encodeDictionary(df: DataFrame): DictionaryManager = {
    val dm = new DictionaryManager
    dimensions.foreach(f => {
      val selectedDim =
        if (types(f).multiValued) df.select(explode(df(f)))
        else df.select(df(f))
      dm.addToDictionary(f, selectedDim.rdd.map(_.getAs[String](0)))
    })
    dm
  }

  //TODO: If index already exist we have to merge dictionary and update indices
  def index(dataframe: DataFrame, time: Time): Unit = {
    val indexPath = location.stripSuffix("/") + "/" + INDEX_PREFIX
    val dictionaryPath = location.stripSuffix("/") + "/" + DICTIONARY_PREFIX

    val path = new HadoopPath(indexPath)
    val conf = new Configuration
    val fs = FileSystem.get(path.toUri, conf)
    if (fs.exists(path)) {
      log.warn(s"deleting indices at location ${path.getName}")
      fs.delete(path, true)
    }
    fs.mkdirs(path)
    FileSystem.closeAll()

    dictionary = encodeDictionary(dataframe)
    val dictionaryBr = dataframe.rdd.context.broadcast(dictionary)

    val parallelism = dataframe.rdd.context.defaultParallelism
    val sparkConf = dataframe.rdd.sparkContext.getConf
    val localDir = new File(DalUtils.getLocalDir(sparkConf))
    log.info(s"Created ${localDir} to write lucene shuffle")

    val inSchema = dataframe.schema

    dataframe.coalesce(parallelism).rdd.mapPartitionsWithIndex((i, itr) => {
      val indexWriterConfig = new IndexWriterConfig(analyzer)
      indexWriterConfig.setOpenMode(OpenMode.CREATE)
      // Open a directory on Standalone/YARN/Mesos disk cache
      val shuffleIndexFile = DalUtils.getTempFile(PREFIX, localDir)
      val shuffleIndexPath = shuffleIndexFile.toPath
      log.info(s"Created ${shuffleIndexPath} to write lucene partition $i shuffle")
      converter.setSchema(inSchema)
      converter.setDictionary(dictionaryBr.value)

      val directory = new MMapDirectory(shuffleIndexPath)
      val indexWriter = new IndexWriter(directory, indexWriterConfig)
      itr.foreach {
        r => {
          try {
            val d = converter.rowToDoc(r)
            indexWriter.addDocument(d);
          } catch {
            case e: Throwable => {
              throw new LuceneDAOException(s"Error with adding row ${r} to document ${e.getStackTraceString}")
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

    dictionary.save(dictionaryPath)(dataframe.rdd.sparkContext)

    log.info("Number of partitions: " + dataframe.rdd.getNumPartitions)
  }

  //TODO: load logic will move to LuceneRDD
  var shards : RDD[LuceneShard] = _

  def load(sc: SparkContext): Unit = {
    val indexPath = location.stripSuffix("/") + "/" + INDEX_PREFIX
    val dictionaryPath = location.stripSuffix("/") + "/" + DICTIONARY_PREFIX

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

    val sparkConf = sc.getConf
    val localDir = new File(DalUtils.getLocalDir(sparkConf))

    log.info(s"Created ${localDir} to read lucene shuffle")

    shards = partitionIds.flatMap((i: Int) => {
      val hdfsPath = indexPath + "/" + SUFFIX + i
      // Open a directory on Standalone/YARN/Mesos disk cache
      val shuffleIndexFile = DalUtils.getTempFile(PREFIX, localDir)
      val shuffleIndexPath = shuffleIndexFile.toPath
      log.info(s"Created ${shuffleIndexPath} to read lucene partition $i shuffle")

      val conf = new Configuration
      val shard: Option[LuceneShard] = {
        log.info(s"Copying data from deep storage: ${hdfsPath} to local shuffle: ${shuffleIndexPath}")
        try {
          FileSystem.get(conf).copyToLocalFile(false,
            new HadoopPath(hdfsPath),
            new HadoopPath(shuffleIndexPath.toString))
          val directory = new MMapDirectory(shuffleIndexPath)
          val reader = DirectoryReader.open(directory)
          Some(LuceneShard(reader, converter))
        } catch {
          case e: IOException =>
            throw new LuceneDAOException(s"Copy from: ${hdfsPath} to local shuffle: ${shuffleIndexPath} failed")
          case x: Throwable => throw new RuntimeException(x)
        }
      }
      shard
    })
    shards.cache()
    log.info("Number of shards: " + shards.count())
  }

  lazy val qp = new QueryParser("content", analyzer)

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

  //search a query and retrieve for all dimensions + measures
  def search(queryStr: String, sample: Double = 1.0) : RDD[Row] = {
    search(queryStr, types.keys.toSeq, sample)
  }

  def count(queryStr: String): Int = {
    if(shards == null) throw new LuceneDAOException(s"count called with null shards")
    shards.map((shard: LuceneShard) => {
      shard.count(qp.parse(queryStr))
    }).sum().toInt
  }

  //TODO: Aggregator will be instantiated based on the operator and measure
  def getAggregator(measure: String): LuceneAggregator = {
    if (converter.types(measure).dataType == StringType)
      new CardinalityEstimator
    else
      new NumericSum
  }

  //TODO: If combOp is not within function scope, agg broadcast does not happen and NPE is thrown
  //TODO: Look into treeAggregate
  def combOp = (agg: LuceneAggregator,
                other: LuceneAggregator) => {
    agg.merge(other)
  }

  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String): Array[Int] = {
    if (shards == null) throw new LuceneDAOException(s"timeseries called with null shards")

    val dimSize = Math.floor((maxTime - minTime) / rollup).toInt
    log.info(s"calculated time series size ${dimSize} from [$maxTime, $minTime] with rollup $rollup")

    val seqOp = (agg: LuceneAggregator,
                 shard: LuceneShard) => {
      shard.timeseries(
        queryStr,
        minTime,
        maxTime,
        rollup,
        measure,
        agg)
    }

    val agg = getAggregator(measure)

    agg.init(dimSize)

    val results = shards.treeAggregate(agg)(seqOp, combOp)

    if (converter.types(measure).dataType == StringType)
      results.asInstanceOf[CardinalityEstimator].buffer.map(_.cardinality().toInt)
    else
      results.asInstanceOf[NumericSum].buffer.map(_.toInt)
  }

  def group(queryStr: String,
            dimension: String,
            measure: String) : Array[Int] = {
    if(shards == null) throw new LuceneDAOException(s"group called with null shards")

    val dimRange = dictionary.getRange(dimension)
    val dimOffset = dimRange._1
    val dimSize = dimRange._2 - dimRange._1 + 1

    val seqOp = (agg: LuceneAggregator,
                 shard: LuceneShard) => {
      shard.group(
        queryStr,
        dimension,
        dimOffset,
        measure,
        agg = agg)
    }

    //TODO: Aggregator is picked based on the SQL functions sum, countDistinct, count
    val agg = getAggregator(measure)

    //TODO: If aggregator is not initialized from driver and broadcasted, merge fails on NPE
    //TODO: RDD aggregate needs to be looked into
    agg.init(dimSize)

    val results = shards.treeAggregate(agg)(seqOp, combOp)

    //TODO: Map dimension using dictionary
    if (converter.types(measure).dataType == StringType)
      results.asInstanceOf[CardinalityEstimator].buffer.map(_.cardinality().toInt)
    else
      results.asInstanceOf[NumericSum].buffer.map(_.toInt)
  }
}


