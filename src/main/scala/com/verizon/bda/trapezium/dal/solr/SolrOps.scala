package com.verizon.bda.trapezium.dal.solr

import java.nio.file.{Path, Paths}
import java.sql.Time

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider

import scala.collection.JavaConverters._
import scala.xml.XML
import scalaj.http.{Http, HttpResponse}

/**
  * Created by venkatesh on 8/3/17.
  */
abstract class SolrOps(solrMap: Map[String, String]) {

  val cloudClient: ZkClientClusterStateProvider = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  val appName = solrMap("appName")
  var aliasCollectionName: String = null
  var collectionName: String = null
  lazy val configName = s"$appName/${aliasCollectionName}"
  lazy val zkHosts = solrMap("zkHosts").split(",").toList.asJava
  var indexFilePath: String = null
  var coreMap: Map[String, String] = null

  def getSolrNodes: List[String] = {
    cloudClient.liveNodes()
      .asScala.toList
      .map(p => p.split("_")(0))
      .map(p => p.split(":")(0))
  }

  def getSolrclient(): ZkClientClusterStateProvider = {
    val chroot = solrMap("zroot")
    new ZkClientClusterStateProvider(zkHosts, chroot)


  }

  def unloadCore(node: String, coreName: String): Boolean = {
    log.info("unloading core")
    val port = solrMap("solrPort")
    val solrServerUrl = "http://" + node + s":${port}/solr/admin/cores"
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

  def getSolrCollectionurl(): String = {
    val host = getSolrNodes.head
    val port = solrMap("solrPort")
    val solrServerUrl = "http://" + host + s":${port}/solr/admin/collections"
    solrServerUrl
  }

  def aliasCollection(): Unit = {
    val solrServerUrl = getSolrCollectionurl()
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    log.info(s"aliasing collection  ${collectionName} with ${aliasCollectionName}")
    val response: scalaj.http.HttpResponse[String] = url.param("collections", collectionName)
      .param("name", aliasCollectionName)
      .param("action", "CREATEALIAS").asString
    log.info(s"aliasing collection response ${response.body}")

  }

  def deleteCollection(collectionName: String): Unit = {
    val solrServerUrl = getSolrCollectionurl()
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    log.info(s"deleting collection${collectionName} if exists")
    val response1: scalaj.http.HttpResponse[String] = url.param("name", collectionName)
      .param("action", "DELETE").asString
    log.info(s"deleting collection response ${response1.body}")
  }

  def createCollection(): Unit = {
    val solrServerUrl = getSolrCollectionurl
    val url = Http(solrServerUrl).timeout(connTimeoutMs = 20000, readTimeoutMs = 50000)
    deleteCollection(collectionName)
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
      .param("router.name", "compositeId")
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
      case "HDFS" => {
        val set = Set("appName", "zkHosts", "nameNode", "zroot", "storageDir",
          "solrConfig", "numShards", "replicationFactor", "solrPort")
        set.foreach(p =>
          if (!params.contains(p)) {
            throw new SolrOpsException(s"Map Doesn't have ${p} map should contain ${set}")
          })
        new SolrOpsHdfs(params)
      }
      case "LOCAL" => {
        val set = Set("appName", "zkHosts", "nameNode", "solrNodePassword", "solrUser",
          "folderPrefix", "zroot", "storageDir", "solrConfig", "solrPort")
        set.foreach(p =>
          if (!params.contains(p)) {
            throw new SolrOpsException(s"Map Doesn't have ${p} map should contain ${set}")
          })
        new SolrOpsLocal(params)
      }
    }
  }


}