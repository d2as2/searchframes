package com.verizon.bda.trapezium.dal.solr

import java.nio.file.{Path, Paths}
import java.util

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.joda.time.LocalDate

import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpResponse}

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOps(config: Config, map: Map[String, ListBuffer[String]]) {
  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOps])
  val localDate = new LocalDate().toString("YYYY_MM_dd")
  val appName = config.getString("appName")
  val collectionPrefix = config.getString("collection_prefix")
  val aliasCollectionName = config.getString("aliasName")
  lazy val collectionName = s"${collectionPrefix}_${localDate}"

  val configName = s"$appName/${aliasCollectionName}"

  def getSolrclient(): CloudSolrClient = {
    val zkHosts = config.getStringList("zkHosts")
    val chroot = config.getString("zroot")
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient(zkHosts, chroot)
    cloudSolrClient
  }

  def createCollection(config: Config): Unit = {
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
  }

  def createCores(): Unit = {
    var i = 1
    for ((host, fileList) <- map) {
      for (directory <- fileList.toList) {
        val id = directory.split("-").last
        val coreName = s"${collectionName}_shard${id}_replica1"
        val solrServerUrl = ("http://" + host + ":8983/solr/admin/cores")
        val url = Http(solrServerUrl)
        var reponse: HttpResponse[String] = null
        var retry = 0
        do {
          unloadCore(host, coreName)
          reponse = url.param("action", "CREATE")
            .param("name", coreName)
            .param("dataDir", directory)
            .param("collection", collectionName)
            .param("shard", s"shard${i}")
            .param("collection.configName", configName)
            .param("numshard", id + 1)
            .asString

          retry = retry + 1
        } while (reponse.is2xx && retry < 3)
        i = i + 1
        log.info(s"body:${reponse.body} \nresponse status:${reponse.statusLine}" +
          s" \nadditional${reponse.headers("status")} ")
      }
    }
  }

  def aliasCollection(config: Config): Unit = {
    val coreCreate = new CollectionAdminRequest.CreateAlias
    coreCreate.setAliasedCollections(collectionName)
    coreCreate.setAliasName(aliasCollectionName)
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)
  }

  def reloadCollection(aliasedCollection: String): Unit = {
    val req = new CollectionAdminRequest.Reload
    req.setCollectionName(aliasedCollection)
    req.process(cloudClient)
  }

  def unloadCore(node: String, coreName: String): Boolean = {
    val solrServerUrl = ("http://" + node + ":8983/solr/admin/cores")
    val url = Http(solrServerUrl)
    val response: HttpResponse[String] = url.param("action", "UNLOAD")
      .param("core", coreName)
      .asString
    response.is2xx
  }


  def upload(): Unit = {
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = cloudClient
    val appName = config.getString("appName")
    val path: Path = Paths.get(config.getString("solrConfig"))

    log.info(s"uploading to ${configName} from path:${path.toString}")
    solrClient.uploadConfig(path, configName)

    log.info("uploaded the config successfully ")
  }


}
