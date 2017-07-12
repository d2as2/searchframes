package com.verizon.bda.trapezium.dal.solr

import java.io.File
import java.nio.file.{FileSystems, Path}
import java.util

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.{CloudSolrClient, ZkClientClusterStateProvider}
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder
import org.apache.solr.common.cloud.ZkConfigManager


/**
  * Created by venkatesh on 7/6/17.
  */
class solrZookeeper {
  // @transient lazy val log = Logger.getLogger(classOf[solrZookeeper])

  def upload(config: Config): Unit = {
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = getSolrclient(zkHosts)
    val zkcluster = new ZkClientClusterStateProvider(zkHosts, "/")
    val appName = config.getString("appName")
    val path: Path = FileSystems.getDefault().getPath(s"/solr/$appName")
    val files = config.getList("files").asInstanceOf[List[String]]

    for (file <- files)
      zkcluster.uploadConfig(path, file)

    val configManager = new ZkConfigManager(solrClient.getZkStateReader.getZkClient)
   // require(configManager.listConfigs.containsAll(files),
    // "has succuesully moved all files required for solr")

  }

  def getSolrclient(zkHosts: util.List[String]): CloudSolrClient = {
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient.Builder().withZkHost(zkHosts).build()
    cloudSolrClient
  }
}
