package com.verizon.bda.trapezium.dal.lucene

import java.io.File
import java.nio.file.FileSystems

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, FilterLeafReader}
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.lucene.document.Document

/**
 * @author debasish83 on 12/12/16.
 */
class LuceneRDD(sc: SparkContext) extends RDD[LucenePartition] with Serializable {
  override def compute(split: Partition, context: TaskContext)
}

/** Factory for [[LuceneRDD]] instancs */
object LuceneRDD {
  /**
   * Converts an input RDD of documents to LuceneRDD
   *
   * @param inputDocs The input RDD of lucene documents.
   * @return The LuceneRDD
   */
  def apply(inputDocs: RDD[Document]): LuceneRDD = {
    inputDocs.mapPartitionsWithIndex((i, itr) => {
      /*
      val dir = new File(indexPath + PREFIX + i + File.separator)
      FileUtils.forceDeleteOnExit(dir)
      if (dir.isDirectory()) {
        FileUtils.cleanDirectory(dir)
      }
      */
      val indexWriterConfig = new IndexWriterConfig(analyzer)
      indexWriterConfig.setOpenMode(OpenMode.CREATE)
      val localPath = FileSystems.getDefault().getPath(indexPath, PREFIX + i)
      val directory = new MMapDirectory(localPath)

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
      if (env != "local") {
        FileSystem.get(conf).copyFromLocalFile(true, true,
          new Path(indexPath, PREFIX + i), new Path(hdfsPath))
      }
      else {
        FileSystem.get(conf).copyFromLocalFile(true, true,
          new Path(indexPath, PREFIX + i), new Path(hdfsPath, PREFIX + i))
      }
      Iterator.empty
    }).count()
    ???
  }

  private[dal] def createLucenePartition()
}