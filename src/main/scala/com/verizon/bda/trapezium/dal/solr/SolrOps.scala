package com.verizon.bda.trapezium.dal.solr

import java.io.File
import java.nio.file.{FileSystems, Path, Paths}
import java.util

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, CoreAdminRequest}
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.joda.time.LocalDate
import org.zeroturnaround.zip.ZipUtil

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOps(config: Config) {
  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOps])
  var alaiasedCollectionName = ""
  var configName = ""

  def createCore(): Unit = {
    val coreCreate = new CoreAdminRequest.Create()
    coreCreate.setCoreNodeName("")
    coreCreate.setDataDir("")
    coreCreate.setCollectionConfigName(configName)
    coreCreate.setCollection("")
    coreCreate.setShardId("")
    coreCreate.setNumShards(1)
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)
  }

  def aliasCollection(config: Config): Unit = {

    val coreCreate = new CollectionAdminRequest.CreateAlias
    coreCreate.setAliasedCollections(alaiasedCollectionName)
    coreCreate.setAliasName(config.getString("aliasName"))
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)

  }

  def reloadCollection(aliasedCollection: String): Unit = {
    val req = new CollectionAdminRequest.Reload
    req.setCollectionName(aliasedCollection)
    req.process(cloudClient)
  }


  def createCollection(config: Config): Unit = {
    val replication = config.getInt("replication")
    val maxShardsPerNode = config.getInt("maxShardsPerNode")
    val maxShards = config.getInt("maxShardsPerNode")
    val solrConfigName = configName
    val req = new CollectionAdminRequest.Create()
    val localDate = new LocalDate().toString("YYYY_MM_DD")
    alaiasedCollectionName = s"sps_${localDate}"
    val response: CollectionAdminResponse = req.setCollectionName(alaiasedCollectionName)
      .setReplicationFactor(replication)
      .setMaxShardsPerNode(maxShardsPerNode)
      .setNumShards(maxShards)
      .setConfigName(solrConfigName)
      .process(cloudClient)

    log.info(response.getResponse.asMap(5).toString)
  }

  def upload(collectionName: String): Unit = {
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = cloudClient
    val appName = config.getString("appName")
    val path: Path = Paths.get(config.getString("solrConfig"))
    configName = s"$appName/${collectionName}"
    val temp = config.getString("tempDir")
    ZipUtil.pack(new File(config.getString("solrConfig") + "/conf"),
      new File(s"${temp}/conf_1.zip"))
    solrClient.uploadConfig(path, configName)
    solrClient.downloadConfig(configName, Paths.get(temp))
    ZipUtil.pack(new File(s"${temp}/conf"),
      new File(s"${temp}/conf_2.zip"))
    require(ZipUtil.archiveEquals(new File(s"${temp}/conf_2.zip"),
      new File(s"${temp}/conf_1.zip")), "upload conf directory not successful")

  }


  def getSolrclient(): CloudSolrClient = {
    val zkHosts = config.getStringList("zkHosts")
    // new CloudSolrClient.Builder().withZkHost(zkHosts).build()
    val chroot = "/solr"
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient(zkHosts, chroot)
    cloudSolrClient
  }
}
