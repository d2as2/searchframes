package com.verizon.bda.trapezium.dal.solr

import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.xml.XML
import scalaj.http.{Http, HttpResponse}

/**
  * Created by venkatesh on 8/3/17.
  */
class SolrOpsHdfs(config: Config) extends SolrOps(config: Config) {
  override lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  var map: Map[String, String] = null

  override def createCollection(): Unit = {
    val solrConfigName = configName

    val host = config.getStringList("solrNodeHosts").get(0)
    val solrServerUrl = "http://" + host + ":8983/solr/admin/collections"
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
    log.info(s"deleting collection${collectionName} if exists")
    val response1: HttpResponse[String] = url.param("name", collectionName).param("action", "DELETE").asString
    log.info(s"deleting collection response ${response1.body}")
    log.info(s"creating collection : ${collectionName} ")

    val response: HttpResponse[String] = url.param("action", "CREATE")
      .param("name", collectionName)
      .param("numShards", "10")
      .param("replicationFactor", "1")
      .param("maxShardsPerNode", "4")
      .param("collection.configName", configName).asString
    log.info(s"creating collection response ${response.body}")
    val xmlBody = XML.loadString(response.body)
    val ips = (xmlBody \\ "lst").map(p => p \ "@name").map(_.text.split("_")(0)).filter(p => p != "responseHeader" && p != "success")

    val coreNames = (xmlBody \\ "str").map(p => p.text)
    map = (coreNames, ips).zipped.toMap
    for ((k, v) <- map) {
      log.info(s"coreName:  ${k} ip ${v}")
    }
    log.info(map)

  }

  override def createCores(): Unit = {
    var reponse: HttpResponse[String] = null
    val nameNode = config.getString("nameNode")
    val hdfsHome = s"hdfs://${nameNode}"
    val indexedPath = config.getString("indexFilesPath")
    log.info("creating cores")
    for ((coreName, host) <- map) {
      unloadCore(host, coreName)
      log.info(s"creating core:${coreName}")
      var retry = 0
      //do {
        val solrServerUrl = (s"http://${host}/solr/admin/cores")
        val url = Http(solrServerUrl).timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
        val tmp = coreName.split("_")
        val shard_index = tmp(tmp.size - 2).substring(5).toInt
        val directory = s"$hdfsHome$indexedPath/part-${shard_index - 1}"

        reponse = url.param("action", "CREATE")
          .param("name", coreName)
          .param("dataDir", directory)
          .param("collection", collectionName)
          .param("collection.configName", configName)
          //          .param("shard", s"shard$shard_index")
          //          .param("numshard", s"$shard_index" )
          .asString
        Thread.sleep(1000)
        log.info(s"creating core response ${reponse.body} with coreName:${coreName} and directory${directory}")
        retry = retry + 1
     // } while (!reponse.is2xx && retry < 3)
    }

  }
}
