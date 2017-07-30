package com.verizon.bda.trapezium.dal.solr

import java.nio.file.{Path, Paths}
import java.util
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, CoreAdminRequest}
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.joda.time.LocalDate
import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOps(config: Config, map: Map[String, ListBuffer[String]]) {
  val cloudClient: CloudSolrClient = getSolrclient()
  lazy val log = Logger.getLogger(classOf[SolrOps])
  val localDate = new LocalDate().toString("YYYY_MM_dd")
  val appName = config.getString("appName")

  lazy val alaiasedCollectionName = s"${appName}_${localDate}"

  var configName = ""

  def createCores(): Unit = {

    val localDate = new LocalDate().toString("YYYY_MM_dd")
    //    val dataDir = config.getString("dataDir")
    //    val directory = s"${dataDir}_${localDate}
    // "
    var i = 1
    for ((k, v) <- map) {
      val solrServer = new HttpSolrClient(k + ":8983/solr")

      for (directory <- v.toList) {

        val coreCreate = new CoreAdminRequest.Create()
        val id = directory.split("-").last
        val name = s"${alaiasedCollectionName}_shard${i}_replica1"
        val coreName = s"${alaiasedCollectionName}_shard${id}_replica1"
        coreCreate.setCoreName(name)
        coreCreate.setDataDir(directory)
        coreCreate.setCollectionConfigName(configName)
        coreCreate.setCollection(alaiasedCollectionName)
        coreCreate.setShardId(s"shard${i}")
        coreCreate.setCoreNodeName(coreName)
        val response = coreCreate.process(cloudClient)
        i = i + 1
        Thread.sleep(1000)
        log.info(s"created core with name:${name} in collection ${alaiasedCollectionName}" +
          s"on host: ${k} on data dir ${directory} optained response ${response}")
      }
      solrServer.close()
    }
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
    val maxShards = config.getInt("maxShards")
    val solrConfigName = configName
    val req = new CollectionAdminRequest.Create()
    log.info(s"creating collection : ${alaiasedCollectionName} with shards per")
    val response: CollectionAdminResponse = req.setCollectionName(alaiasedCollectionName)
      .setReplicationFactor(replication)
      .setMaxShardsPerNode(maxShardsPerNode)
      .setNumShards(maxShards)
      .setConfigName(solrConfigName)
      .process(cloudClient)

    log.info(s"created collection :${alaiasedCollectionName} "
      + response.getResponse.asMap(5).toString)
  }

  def upload(collectionName: String): Unit = {
    val zkHosts: util.List[String] = config.getStringList("zkHosts")
    val solrClient = cloudClient
    val appName = config.getString("appName")
    val path: Path = Paths.get(config.getString("solrConfig"))
    configName = s"$appName/${collectionName}"
    val temp1 = config.getString("tempDir1")
    val temp2 = config.getString("tempDir2")
    //    ZipUtil.pack(new File(config.getString("solrConfig") ),
    //      new File(s"${temp1}/conf.zip"))
    solrClient.uploadConfig(path, configName)
    //    solrClient.downloadConfig(configName, Paths.get(temp2))
    //    ZipUtil.pack(new File(s"${temp2}"),
    //      new File(s"${temp2}/conf.zip"))
    //    require(ZipUtil.archiveEquals(new File(s"${temp1}/conf.zip"),
    //      new File(s"${temp2}/conf.zip")), "upload conf directory not successful")
    log.info("uploaded the config successfully ")
  }


  def getSolrclient(): CloudSolrClient = {
    val zkHosts = config.getStringList("zkHosts")
    // new CloudSolrClient.Builder().withZkHost(zkHosts).build()
    val chroot = config.getString("zroot")
    val cloudSolrClient: CloudSolrClient = new CloudSolrClient(zkHosts, chroot)
    cloudSolrClient
  }
}
