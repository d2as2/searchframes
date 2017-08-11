package com.verizon.bda.trapezium.dal.solr

import java.sql.Time
import java.util.Calendar


/**
  * Created by venkatesh on 7/27/17.
  */
object SolrTests {
  def main(args: String): Unit = {

    // hdfs
    val ops1 = SolrOps("Hdfs", Map(
      "appName" -> "palomar",
      "zkHosts" -> "md-bdadev-79.verizon.com,md-bdadev-80.verizon.com,md-bdadev-82.verizon.com",
      "nameNode" -> "md-bdadev-15.verizon.com:8020",
      "zroot" -> "/solr",
      "storageDir" -> "/data0/tmp/lucene/",
      "solrConfig" -> "/data0/poosave/solrUploadTest/daily_hdfs/conf",
      "numShards" -> "10",
      "replicationFactor" -> "1"
    ))
    val indexFilesPath1 = "/user/palomar/luceneindex/solr/sps_daily_hdfs4"
    val cal1 = Calendar.getInstance()
    ops1.makeSolrCollection("sps_current_hdfs_test", indexFilesPath1,
      new Time(cal1.getTimeInMillis()))
    // local

    val ops = SolrOps("Local", Map(
      "appName" -> "palomar",
      "zkHosts" -> "md-bdadev-79.verizon.com,md-bdadev-80.verizon.com,md-bdadev-82.verizon.com",
      "nameNode" -> "md-bdadev-15.verizon.com:8020",
      "solrNodePassword" -> "31Venkatesh",
      "solrUser" -> "poosave",
      "folderPrefix" -> "/part-2",
      "zroot" -> "/solr",
      "storageDir" -> "/data0/tmp/lucene/",
      "solrConfig" -> "/data0/poosave/solrUploadTest/contextSensitiveSolrConfigs_daily/conf",
      "numShards" -> "10",
      "replicationFactor" -> "1"
    ))
    val indexFilesPath = "/user/will/orionprocessed/subprofile/luceneIndex/2017-07-18" +
      "/dailyView/index"

    val cal = Calendar.getInstance()
    ops.makeSolrCollection("sps_current_test", indexFilesPath, new Time(cal.getTimeInMillis()))


  }
}