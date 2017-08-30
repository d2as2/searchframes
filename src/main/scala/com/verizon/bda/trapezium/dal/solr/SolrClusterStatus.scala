package com.verizon.bda.trapezium.dal.solr

import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 8/28/17.
  */
case class SolrCollectionStatus(machine: String, state: String, collectionName: String,
                                configName: String, coreName: String, shard: String)

object SolrClusterStatus {
  var zkHostList: String = _
  // "listOfzookeepers"
  var chroot: String = _
  var collectionName: String = _

  // "zroot"
  def apply(zkList: String, zroot: String, collection: String): Unit = {
    zkHostList = zkList
    chroot = zroot
    collectionName = collection
  }

  lazy val cloudClient: ZkClientClusterStateProvider =
    new ZkClientClusterStateProvider(zkHosts, chroot)

  lazy val solrResponseBody = getClusterStatus()

  // solrMap("zkHosts")
  lazy val zkHosts = zkHostList.split(",").toList.asJava

  def getSolrNodes: List[String] = {
    cloudClient.liveNodes()
      .asScala.toList
      .map(p => p.split("_")(0))
  }

  def getSolrclient(): ZkClientClusterStateProvider = {
    new ZkClientClusterStateProvider(zkHosts, chroot)
  }

  def getClusterStatus(): String = {
    val node: String = getSolrNodes(0)
    val url = s"http://$node/solr/admin/collections?action=CLUSTERSTATUS" +
      s"&wt=json&collection=$collectionName"
    SolrOps.makeHttpRequest(url)
  }

  def parseSolrResponse(): List[SolrCollectionStatus] = {
    val lb = new ListBuffer[SolrCollectionStatus]

    // Convert JSON string to JSONObject
    val tomJsonObject = new JSONObject(solrResponseBody)
    val test = tomJsonObject.get("cluster").asInstanceOf[JSONObject]
      .get("collections").asInstanceOf[JSONObject]
    val collectionName = test.keys().next().toString
    val configName = test.get(collectionName).asInstanceOf[JSONObject]
      .get("configName").toString

    val shardsNodes = test.get(collectionName).asInstanceOf[JSONObject]
      .get("shards").asInstanceOf[JSONObject]
    val shardKeys = shardsNodes.keys()

    while (shardKeys.hasNext()) {
      val shard = shardKeys.next().toString
      val coreNodes = shardsNodes.get(shard).asInstanceOf[JSONObject]
        .get("replicas").asInstanceOf[JSONObject]
      val coreKeys = coreNodes.keys()
      while (coreKeys.hasNext) {
        val coreKey = coreKeys.next().toString
        val coreNode = coreNodes.get(coreKey).asInstanceOf[JSONObject]
        val machine = coreNode.get("node_name").toString.split("_")(0)
        val coreName = coreNode.get("core").toString
        val state = coreNode.get("state").toString.toLowerCase

        lb.append(SolrCollectionStatus(machine, state, collectionName,
          configName, coreName, shard))

      }
    }
    lb.toList
  }
}
