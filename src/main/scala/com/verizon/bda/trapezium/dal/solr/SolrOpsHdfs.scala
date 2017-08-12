package com.verizon.bda.trapezium.dal.solr

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger
import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 8/3/17.
  */
class SolrOpsHdfs(solrMap: Map[String, String]) extends SolrOps(solrMap: Map[String, String]) {
  override lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])

  override def createCores(): Unit = {
    val nameNode = solrMap("nameNode")
    val hdfsHome = s"hdfs://${nameNode}"
    log.info("creating cores")
    val list = new ListBuffer[String]
    for ((coreName, host) <- coreMap) {
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
      val request = new HttpGet(url)
      // check for response status (should be 0)

      val response = client.execute(request)
      log.info(s"response: ${response} ")
      client.close()
      response.close()
    })
  }
}
