package com.verizon.bda.trapezium.dal.solr

import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpResponse}

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
    var i = 1

    for ((host, fileList) <- map) {
      for (directory <- fileList.toList) {
        val id = directory.split("-").last.toInt + 1
        val port = solrMap("solrPort")
        val coreName = s"${collectionName}_shard${id}_replica1"
        val solrServerUrl = ("http://" + host + s":${port}/solr/admin/cores")
        val url = Http(solrServerUrl)
        var reponse: HttpResponse[String] = null
        var retry = 0
        do {

          reponse = url.param("action", "CREATE")
            .param("name", coreName)
            .param("dataDir", directory)
            .param("collection", collectionName)
            .param("shard", s"shard${i}")
            .param("collection.configName", configName)
            .param("numshard", s"$id")
            .asString

          retry = retry + 1
        } while (!reponse.is2xx && retry < 3)
        i = i + 1
        log.info(s"body:${reponse.body} \nresponse status:${reponse.statusLine} " +
          s"\nadditional${reponse.headers("status")} ")
      }
    }
  }


}
