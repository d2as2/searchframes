package com.verizon.bda.trapezium.dal.solr

import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException

/**
  * Created by venkatesh on 8/28/17.
  */
object SolrRestore {


  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new SolrOpsException(s"should pass three arguments:" +
        s" movingDir folderPrefix collectionName" +
        s"example: /usr/sample/colectionFolder /part- /dailyView")
    }
    val folderPrefix1 = args(1)
    val movingDirectory = args(0)
    val collection = args(2)
    val folderPrefix = if (folderPrefix1.charAt(0) == '/') {
      folderPrefix1
    } else {
      "/" + folderPrefix1
    }
    SolrClusterStatus("zkList", "zroot", collection)
    val li = SolrClusterStatus.parseSolrResponse()
      .filter(p => !(p.state.equalsIgnoreCase("active")))
    li.foreach(p => {
      SolrOps.unloadCore(p.machine, p.coreName)
    })
    val li2 = li.map(p => {
      val tmp = p.coreName.split("_")
      val partFile = folderPrefix + (tmp(tmp.length - 2).substring(5).toInt - 1)
      val dataDir = s"$movingDirectory$partFile"
      s"http://${p.machine}/solr/admin/cores?" +
        "action=CREATE&" +
        s"collection=${p.collectionName}&" +
        s"collection.configName=${p.configName}&" +
        s"name=${p.coreName}&" +
        s"dataDir=${dataDir}&" +
        s"shard=${p.shard}&" +
        s"wt=json&indent=true"
    })
    SolrOps.makeHttpRequests(li2)
  }


}



