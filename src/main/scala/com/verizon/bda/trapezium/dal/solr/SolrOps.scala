package com.verizon.bda.trapezium.dal.solr

import java.io.File
import java.nio.file.{FileSystems, Path}
import java.util

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, CoreAdminRequest}
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.apache.solr.common.cloud.ZkConfigManager
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOps(config: Config) {
  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOps])

  def createCore(): Unit = {
    val coreCreate = new CoreAdminRequest.Create()
    coreCreate.setCoreNodeName("")
    coreCreate.setDataDir("")
    coreCreate.setCollectionConfigName("")
    coreCreate.setCollection("")
    coreCreate.setShardId("")
    coreCreate.setNumShards(1)
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)
  }

  def aliasCollection(): Unit = {
    val coreCreate = new CollectionAdminRequest.CreateAlias
    coreCreate.setAliasedCollections("")
    coreCreate.setAliasName("")
    val response = coreCreate.process(cloudClient)
    log.info(response.getResponse.asMap(5).toString)

  }

  def createCollection(config: Config): Unit = {
    val replication = config.getInt("replication")
    val maxShardsPerNode = config.getInt("maxShardsPerNode")
    val maxShards = config.getInt("maxShardsPerNode")
    val solrConfigName = config.getString("solrConfigName")
    val req = new CollectionAdminRequest.Create()

    val response: CollectionAdminResponse = req.setCollectionName("foo")
      .setReplicationFactor(replication)
      .setMaxShardsPerNode(maxShardsPerNode)
      .setNumShards(maxShards)
      .setConfigName(solrConfigName)
      .process(cloudClient)

    log.info(response.getResponse.asMap(5).toString)
  }

  def upload(config: Config): Unit = {
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = cloudClient
    val appName = config.getString("appName")
    val path: Path = FileSystems.getDefault().getPath(s"/$appName")
    val confingName = config.getString("solrConfigName")
    solrClient.uploadConfig(path, confingName)

  }

  def getSolrclient(): CloudSolrClient = {
    val zkHosts = config.getStringList("zkHosts")
    // new CloudSolrClient.Builder().withZkHost(zkHosts).build()
    val chroot = "/solr"
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient(zkHosts, chroot)
    cloudSolrClient
  }
}
