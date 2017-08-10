package com.verizon.bda.trapezium.dal.solr

import java.nio.file.{Path, Paths}
import java.sql.Time

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest

import scala.collection.JavaConverters._
import scalaj.http.{Http, HttpResponse}

/**
  * Created by venkatesh on 8/3/17.
  */
abstract class SolrOps(solrMap: Map[String, String]) {

  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  val appName = solrMap("appName")
  var aliasCollectionName: String = null
  var collectionName: String = null
  lazy val configName = s"$appName/${aliasCollectionName}"
  lazy val zkHosts = solrMap("zkHosts").split(",").toList.asJava
  var indexFilePath: String = null

  def getSolrNodes: List[String] = {
    cloudClient.getZkStateReader
      .getClusterState
      .getLiveNodes
      .asScala.toList
      .map(p => p.split("_")(0))
      .map(p => p.split(":")(0))
  }

  def getSolrclient(): CloudSolrClient = {
    val chroot = solrMap("zroot")
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient(zkHosts, chroot)
    cloudSolrClient
  }

  def unloadCore(node: String, coreName: String): Boolean = {
    log.info("unloading core")
    val solrServerUrl = if (node.contains("8983")) {
      "http://" + node + "/solr/admin/cores"
    }
    else {
      "http://" + node + ":8983/solr/admin/cores"
    }
    val url = Http(solrServerUrl)
    val response: HttpResponse[String] = url.param("action", "UNLOAD")
      .param("core", coreName)
      .asString
    log.info(s"unloading core response ${response.body}")
    response.is2xx
  }

  def upload(): Unit = {
    val solrClient = cloudClient
    val path: Path = Paths.get(solrMap("solrConfig"))
    log.info(s"uploading to ${configName} from path:${path.toString}")
    solrClient.uploadConfig(path, configName)
    log.info("uploaded the config successfully ")
  }

  def aliasCollection(): Unit = {
    val coreCreate = new CollectionAdminRequest.CreateAlias
    coreCreate.setAliasedCollections(collectionName)
    coreCreate.setAliasName(aliasCollectionName)
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)
  }

  def deleteCollection(host: String, collectionName: String): Unit = {
    val solrServerUrl = "http://" + host + ":8983/solr/admin/collections"
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    log.info(s"deleting collection${collectionName} if exists")
    val response1: scalaj.http.HttpResponse[String] = url.param("name", collectionName)
      .param("action", "DELETE").asString
    log.info(s"deleting collection response ${response1.body}")
  }

  def createCollection(): Unit

  def createCores(): Unit

  def makeSolrCollection(aliasName: String, hdfsPath: String, workflowTime: Time): Unit = {
    this.aliasCollectionName = aliasName
    this.indexFilePath = hdfsPath
    collectionName = s"${aliasCollectionName}_${workflowTime.getTime.toString}"

    upload()
    createCollection()
    createCores()
    aliasCollection()
  }
}

object SolrOps {
  def apply(mode: String,
            params: Map[String, String]): SolrOps = {
    mode.toUpperCase() match {
      case "HDFS" => new SolrOpsHdfs(params)
      case "LOCAL" => new SolrOpsLocal(params)
    }
  }


}