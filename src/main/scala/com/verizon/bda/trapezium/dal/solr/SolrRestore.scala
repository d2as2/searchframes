package com.verizon.bda.trapezium.dal.solr

import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import scopt.OptionParser

/**
  * Created by venkatesh on 8/28/17.
  */

case class SolrRestore(movingDirectory: String = null, folderPrefix1: String = null,
                       zkList: String = null, zroot: String = null, collection: String = null)

object SolrRestore {


  def main(args: Array[String]): Unit = {

    val parser: OptionParser[SolrRestore] = new OptionParser[SolrRestore]("ApplicationManager") {
      head("SolrRestore used for used for restoring the collection to original state")
      opt[String]("dir")
        .text(s"moving directory")
        .required
        .action((x, c) => c.copy(movingDirectory = x))
      opt[String]("zkList")
        .text(s"zklist used")
        .required
        .action((x, c) => c.copy(zkList = x))
      opt[String]("zroot")
        .text(s"workflow to run")
        .required
        .action((x, c) => c.copy(zroot = x))
      opt[String]("folderPrefix")
        .text(s"folder prefix used")
        .required
        .action((x, c) => c.copy(folderPrefix1 = x))
      opt[String]("collection")
        .text(s"collection to be restored")
        .required
        .action((x, c) => c.copy(collection = x))
    }

    val solrRestore = parser.parse(args, SolrRestore()).get
    val folderPrefix1 = solrRestore.folderPrefix1
    val movingDirectory = solrRestore.movingDirectory
    val collection = solrRestore.collection
    val folderPrefix = if (folderPrefix1.charAt(0) == '/') {
      folderPrefix1
    } else {
      "/" + folderPrefix1
    }
    SolrClusterStatus(solrRestore.zkList, solrRestore.zroot, collection)
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



