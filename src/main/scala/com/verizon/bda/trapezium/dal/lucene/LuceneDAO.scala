package com.verizon.bda.trapezium.dal.lucene

import java.io.IOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath, Path, PathFilter, FileStatus, FileSystem}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.index._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import java.sql.Time
import java.io.File
import org.apache.spark.util.DalUtils

class LuceneDAO(val location: String,
                val dimensions: Seq[String],
                val uniqueKey: Seq[String] = None,
                storageLevel: StorageLevel = StorageLevel.DISK_ONLY) extends Serializable {

  @transient lazy val log = Logger.getLogger(classOf[LuceneDAO])

  lazy val analyzer = new KeywordAnalyzer
  var converter: SparkLuceneConverter = _

  val PREFIX = "trapezium-lucenedao-"
  val SUFFIX = "part-"

  val INDEX_PREFIX = "index"
  val DICTIONARY_PREFIX = "dictionary"

  def setConverter(converter: SparkLuceneConverter): this.type = {
    this.converter = converter
    this
  }

  //TODO: If index already exist we have to merge dictionary and update indices
  def index(dataframe: DataFrame, time: Time): Unit = {
    val indexPath = location.stripSuffix("/") + INDEX_PREFIX
    val path = new HadoopPath(indexPath)
    val conf = new Configuration
    val fs = FileSystem.get(path.toUri, conf)
    if (fs.exists(path)) {
      log.warn(s"deleting indices at location ${path.getName}")
      fs.delete(path, true)
    }
    fs.mkdirs(path)
    FileSystem.closeAll()

    val parallelism = dataframe.rdd.context.defaultParallelism
    val sparkConf = dataframe.rdd.sparkContext.getConf

    dataframe.coalesce(parallelism).rdd.mapPartitionsWithIndex((i, itr) => {
      val indexWriterConfig = new IndexWriterConfig(analyzer)
      indexWriterConfig.setOpenMode(OpenMode.CREATE)
      // Open a directory on Standalone/YARN/Mesos disk cache
      val localDir = new File(DalUtils.getLocalDir(sparkConf))
      val indexDir = File.createTempFile(PREFIX, SUFFIX + i, localDir)
      val directory = new MMapDirectory(indexDir.toPath)
      val indexWriter = new IndexWriter(directory, indexWriterConfig)
      itr.foreach {
        r => {
          try {
            val d = converter.rowToDoc(r)
            indexWriter.addDocument(d);
          } catch {
            case e: Throwable => {
              log.error(s"Error with row: ${r}")
              throw new RuntimeException(e)
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
      val srcPath = new HadoopPath(indexDir.toString)
      FileSystem.get(conf).copyFromLocalFile(true, srcPath, dstPath)
      Iterator.empty
    }).count()

    val filesList = FileSystem.get(conf).listFiles(new HadoopPath(location), true)
    while (filesList.hasNext())
      log.debug(filesList.next().getPath.toString())

    log.info("Number of partitions: " + dataframe.rdd.getNumPartitions)
  }

  var shards : RDD[LuceneShard] = _

  //TODO: load logic will move to LuceneRDD
  def load(sc: SparkContext): Unit = {
    val indexPath = location.stripSuffix("/") + INDEX_PREFIX
    val indexDir = new HadoopPath(indexPath)
    val fs = FileSystem.get(indexDir.toUri, sc.hadoopConfiguration)
    val status: Array[FileStatus] = fs.listStatus(indexDir, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.startsWith("part-")
      }
    })
    val numPartitions = status.length
    val partitionIds = sc.parallelize((0 until numPartitions).toList, sc.defaultParallelism)

    val sparkConf = sc.getConf

    shards = partitionIds.flatMap((i: Int) => {
      val hdfsPath = indexPath + PREFIX + i + File.separator

      val localDir = new File(DalUtils.getLocalDir(sparkConf))
      val indexDir = File.createTempFile(PREFIX, SUFFIX + i, localDir)

      val conf = new Configuration
      val shard: Option[LuceneShard] = {
        log.debug("Copying data from hdfs: " + hdfsPath + " to local: " + indexDir.toString)
        try {
          FileSystem.get(conf).copyToLocalFile(false,
            new HadoopPath(hdfsPath),
            new HadoopPath(indexDir.toString))
          val directory = new MMapDirectory(indexDir.toPath)
          val reader = DirectoryReader.open(directory)
          Some(LuceneShard(reader))
        } catch {
          case e: IOException => None
          case x: Throwable => throw new RuntimeException(x)
        }
      }
      shard
    })

    shards.cache()
    log.info("Number of shards: " + shards.count())
  }

  lazy val qp = new QueryParser("content", analyzer)

  def search(queryStr: String, sample: Double = 1.0): RDD[Row] = {
    val rows = shards.flatMap((shard: LuceneShard) => {
      BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
      val maxRowsPerPartition = Math.floor(sample * shard.getIndexReader.numDocs()).toInt
      val topDocs = shard.search(qp.parse(queryStr), maxRowsPerPartition)

      log.debug("Hits within partition: " + topDocs.totalHits)
      topDocs.scoreDocs.map { d => converter.docToRow(shard.doc(d.doc)) }
    })
    rows
  }

  def count(queryStr: String): Int = {
    shards.map((shard: LuceneShard) => {
      shard.count(qp.parse(queryStr))
    }).sum().toInt
  }
}


