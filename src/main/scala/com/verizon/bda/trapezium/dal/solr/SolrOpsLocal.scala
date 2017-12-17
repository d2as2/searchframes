package com.verizon.bda.trapezium.dal.solr

import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOpsLocal(solrMap: Map[String, String]) extends SolrOps(solrMap: Map[String, String]) {

  override lazy val log = Logger.getLogger(classOf[SolrOpsLocal])
  lazy val movingDirectory = solrMap("storageDir") + collectionName

  lazy val map: Map[String, ListBuffer[(String, String)]] = CollectIndices.
    moveFilesFromHdfsToLocal(solrMap,
      indexFilePath, movingDirectory, coreMap)


  def createCores(): Unit = {
    log.info("inside create cores")
    val list = new ListBuffer[String]
    for ((host, fileList) <- map) {
      for ((directory, coreName) <- fileList.toList) {
        val id = directory.split("-").last.toInt + 1
        val url = s"http://$host/solr/admin/cores?" +
          "action=CREATE&" +
          s"collection=${collectionName}&" +
          s"collection.configName=${configName}&" +
          s"name=${coreName}&" +
          s"dataDir=${directory}&" +
          s"shard=shard${id}&" +
          s"wt=json&indent=true"

        list.append(url)
      }
    }
    log.info(list.toList)
    SolrOps.makeHttpRequests(list.toList)
  }

  override def deleteOldCollections(oldCollection: String): Unit = {
    deleteCollection(oldCollection)
    val oldCollectionDirectory = solrMap("storageDir") + oldCollection
    CollectIndices.deleteDirectory(oldCollectionDirectory)
    CollectIndices.closeSession()

  }
}
