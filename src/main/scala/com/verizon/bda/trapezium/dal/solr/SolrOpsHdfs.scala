package com1

import com.typesafe.config.Config
import com.verizon.bda.trapezium.dal.solr.SolrOps
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scala.xml.XML
import scalaj.http.Http

/**
  * Created by venkatesh on 8/3/17.
  */
class SolrOpsHdfs(config: Config) extends SolrOps(config: Config) {
  override lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  var map: Map[String, String] = null

  override def createCollection(): Unit = {
    val host = config.getStringList("solrNodeHosts").get(0)
    val solrServerUrl = "http://" + host + ":8983/solr/admin/collections"
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    log.info(s"deleting collection${collectionName} if exists")
    val response1: scalaj.http.HttpResponse[String] = url
                                                        .param("name", collectionName)
                                                        .param("action", "DELETE").asString
    log.info(s"deleting collection response ${response1.body}")
    log.info(s"creating collection : ${collectionName} ")

    val response: scalaj.http.HttpResponse[String] = url.param("action", "CREATE")
      .param("name", collectionName)
      .param("numShards", "10")
      .param("replicationFactor", "1")
      .param("maxShardsPerNode", "4")
      .param("collection.configName", configName).asString
    log.info(s"creating collection response ${response.body}")
    val xmlBody = XML.loadString(response.body)
    val ips = (xmlBody \\ "lst").map(p => p \ "@name")
                                .map(_.text.split("_")(0))
                                .filter(p => p != "responseHeader" && p != "success")

    val coreNames = (xmlBody \\ "str").map(p => p.text)
    map = (coreNames, ips).zipped.toMap
    for ((k, v) <- map) {
      log.info(s"coreName:  ${k} ip ${v}")
    }
    log.info(map)

  }

  override def createCores(): Unit = {
    //    var reponse: HttpResponse[String] = null
    val nameNode = config.getString("nameNode")
    val hdfsHome = s"hdfs://${nameNode}"
    val indexedPath = config.getString("indexFilesPath")
    log.info("creating cores")
    val list = new ListBuffer[String]
    for ((coreName, host) <- map) {
      val client = HttpClientBuilder.create().build()

      unloadCore(host, coreName)
      val tmp = coreName.split("_")
      val shard_index = tmp(tmp.size - 2).substring(5).toInt
      val directory = s"$hdfsHome$indexedPath/part-${shard_index - 1}"
      val url = s"http://${host}/solr/admin/cores?" +
        "action=CREATE&" +
        s"collection=${collectionName}&" +
        s"collection.configName=${configName}&" +
        s"name=${coreName}&" +
        s"dataDir=${directory}"
      list.append(url)
    }
    makCoreCreation(list.toList)
  }

  def makCoreCreation(list: List[String]): Unit = {
    list.foreach(url => {
      val client = HttpClientBuilder.create().build()
      println(url)
      val request = new HttpGet(url)
      val response = client.execute(request)
      log.info(s"response: ${response} ")
      client.close()
      response.close()
    })
  }
}
