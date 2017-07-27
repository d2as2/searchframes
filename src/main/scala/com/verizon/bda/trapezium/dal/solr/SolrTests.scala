package com.verizon.bda.trapezium.dal.solr

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by venkatesh on 7/27/17.
  */
object SolrTests {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("solrLucen.conf"))

    val map = ScpIndexFiles.moveFilesFromHdfsToLocal(config)
    val solrOps = new SolrOps(config, map)
    solrOps.upload(config.getString("mothly_collection"))
    solrOps.createCollection(config)
    solrOps.createCores()

  }
}
