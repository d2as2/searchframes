package com.verizon.bda.trapezium.dal.solr

import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOpsLocal(solrMap: Map[String, String]) extends SolrOps(solrMap: Map[String, String]) {

  override lazy val log = Logger.getLogger(classOf[SolrOpsLocal])
  lazy val movingDirectory = solrMap("storageDir") + collectionName
  lazy val map: Map[String, ListBuffer[String]] = CollectIndices.
    moveFilesFromHdfsToLocal(solrMap, getSolrNodes,
      indexFilePath, movingDirectory)


  def createCores(): Unit = {
    log.info("inside create cores")
    val list = new ListBuffer[String]
    for ((host, fileList) <- map) {
      for (directory <- fileList.toList) {
        val id = directory.split("-").last.toInt + 1
        val port = solrMap("solrPort")
        val coreName = s"${collectionName}_shard${id}_replica1"
        val url = s"http://" + host + s":${port}/solr/admin/cores?" +
          "action=CREATE&" +
          s"collection=${collectionName}&" +
          s"collection.configName=${configName}&" +
          s"name=${coreName}&" +
          s"dataDir=${directory}&" +
          s"shard=shard${id}"
        list.append(url)
      }
    }
    makCoreCreation(list.toList)
  }


}
