package com.verizon.bda.trapezium.dal.solr

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.core.CoreContainer
import org.scalatest.FunSuite

/**
  * Created by venkatesh on 8/31/17.
  */
class SolrOpsTest extends FunSuite {

  var solr: EmbeddedSolrServer = _
  val coreContainer = new CoreContainer("solr")
  coreContainer.load()

  def beforeAll() {
    solr = new EmbeddedSolrServer(coreContainer, "test_solr")

  }

}
