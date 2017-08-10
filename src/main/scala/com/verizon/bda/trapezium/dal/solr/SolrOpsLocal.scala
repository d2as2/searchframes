package com.verizon.bda.trapezium.dal.solr

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.CollectionAdminResponse

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

  def createCollection(): Unit = {
    val solrConfigName = configName
    val req = new CollectionAdminRequest.Create()
    log.info(s"creating collection : ${collectionName} with shards per")
    val response: CollectionAdminResponse = req.setCollectionName(collectionName)
      .setReplicationFactor(1)
      .setNumShards(1)
      .setConfigName(solrConfigName)
      .process(cloudClient)

    log.info(s"created collection :${collectionName} "
      + response.getResponse.asMap(5).toString)
    for (host <- map.keySet) {
      val coreName = s"${collectionName}_shard1_replica1"
      unloadCore(host, coreName)
    }
  }

  def createCores(): Unit = {
    var i = 1

    for ((host, fileList) <- map) {
      for (directory <- fileList.toList) {
        val id = directory.split("-").last.toInt + 1

        val coreName = s"${collectionName}_shard${id}_replica1"
        val solrServerUrl = ("http://" + host + ":8983/solr/admin/cores")
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
