package com.verizon.bda.trapezium.dal.solr

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scala.xml.XML
import scalaj.http.Http

/**
  * Created by venkatesh on 8/3/17.
  */
class SolrOpsHdfs(solrMap: Map[String, String]) extends SolrOps(solrMap: Map[String, String]) {
  override lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  var coreMap: Map[String, String] = null

  override def createCollection(): Unit = {
    val host = getSolrNodes.head
    val solrServerUrl = "http://" + host + ":8983/solr/admin/collections"
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    deleteCollection(host, collectionName)
    log.info(s"creating collection : ${collectionName} ")

    val nodeCount = getSolrNodes.size
    val numShards = solrMap("numShards")
    val replicationFactor = solrMap("replicationFactor")
    val maxShardsPerNode = (numShards.toInt * replicationFactor.toInt) / nodeCount + 1
    val url1 = url.param("action", "CREATE")
      .param("name", collectionName)
      .param("numShards", numShards)
      .param("replicationFactor", replicationFactor)
      .param("maxShardsPerNode", maxShardsPerNode.toString)
      .param("collection.configName", configName)
    log.info(s"created url${url1}")
    val response: scalaj.http.HttpResponse[String] = url1.asString

    log.info(s"creating collection response ${response.body}")

    val xmlBody = XML.loadString(response.body)
    // check for response status (should be 0)
    val ips = (xmlBody \\ "lst").map(p => p \ "@name")
      .map(_.text.split("_")(0))
      .filter(p => p != "responseHeader" && p != "success")

//    val map = ((xmlBody \\ "lst" \\ "int" \\ "@name").
//              map(_.text), (xmlBody \\ "lst" \\ "int").
//              map(_.text)).zipped.filter((k, v) => k == "status")

    val coreNames = (xmlBody \\ "str").map(p => p.text)
    coreMap = (coreNames, ips).zipped.toMap
    for ((corename, ip) <- coreMap) {
      log.info(s"coreName:  ${corename} ip ${ip}")
    }
    log.info(coreMap)

  }

  override def createCores(): Unit = {
    //    var reponse: HttpResponse[String] = null
    val nameNode = solrMap("nameNode")
    val hdfsHome = s"hdfs://${nameNode}"
    log.info("creating cores")
    val list = new ListBuffer[String]
    for ((coreName, host) <- coreMap) {
      val client = HttpClientBuilder.create().build()

      unloadCore(host, coreName)

      val tmp = coreName.split("_")
      val shard_index = tmp(tmp.size - 2).substring(5).toInt
      val partindex = shard_index - 1
      val directory = s"$hdfsHome$indexFilePath/part-${partindex}"
      val shard = s"shard${shard_index}"
      val url = s"http://${host}/solr/admin/cores?" +
        "action=CREATE&" +
        s"collection=${collectionName}&" +
        s"collection.configName=${configName}&" +
        s"name=${coreName}&" +
        s"dataDir=${directory}&" +
        s"shard=$shard"
      list.append(url)
    }
    makCoreCreation(list.toList)
  }

  def makCoreCreation(list: List[String]): Unit = {
    list.foreach(url => {
      val client = HttpClientBuilder.create().build()
//      println(url)
      val request = new HttpGet(url)
      // check for response status (should be 0)

      val response = client.execute(request)
      log.info(s"response: ${response} ")
      client.close()
      response.close()
    })
  }
}
