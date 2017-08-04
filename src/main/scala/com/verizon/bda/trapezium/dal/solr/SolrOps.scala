package com.verizon.bda.trapezium.dal.solr

import java.nio.file.{Path, Paths}
import java.util

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.joda.time.LocalDate

import scalaj.http.{Http, HttpResponse}

/**
  * Created by venkatesh on 8/3/17.
  */
abstract class SolrOps(config: Config) {

  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOpsHdfs])
  val localDate = new LocalDate().toString("YYYY_MM_dd")
  val appName = config.getString("appName")
  val collectionPrefix = config.getString("collection_prefix")
  val aliasCollectionName = config.getString("aliasName")
  lazy val collectionName = s"${collectionPrefix}_${localDate}"
  val part0isPresent = false
  val configName = s"$appName/${aliasCollectionName}"

  def getSolrclient(): CloudSolrClient = {
    val zkHosts = config.getStringList("zkHosts")
    val chroot = config.getString("zroot")
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
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = cloudClient
    val appName = config.getString("appName")
    val path: Path = Paths.get(config.getString("solrConfig"))

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

  def createCollection(): Unit

  def createCores(): Unit
}
