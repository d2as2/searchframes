package com.verizon.bda.trapezium.dal.solr

import java.io.FileWriter

import com.typesafe.config.Config

/**
  * Created by venkatesh on 7/17/17.
  */
object SolrSchemaXmlGenerator {

  def makeSchemaXml(config: Config): Unit = {
    val sb = new StringBuilder()
    val fw = new FileWriter("Schema.xml", true)
    val schemaName = config.getString("schemaName")
    val solrVersion = config.getString("solrVersion")
    val uniqueKey = config.getString("uniqueKey")
    try {
     fw.write(s"<schema name=\"${schemaName}\" version=\"${solrVersion}\">\n")
     fw.write("<fields>\n")
     for()
     fw.write("")

     fw.write("</fields>\n")
     fw.write(s"<uniqueKey>${uniqueKey}</uniqueKey>")
     fw.write("<types>\n" +
        "<fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\"/>\n" +
        "<fieldType name=\"long\" class=\"solr.TrieLongField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>\n" +
        "fieldType name=\"double\" class=\"solr.TrieDoubleField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>\n" +
        "<fieldType name=\"string_in_mem_dv\" class=\"solr.StrField\" docValues=\"true\" docValuesFormat=\"Memory\"/>\n" +
        "</types>")
      fw.write("</schema>\n")
    } catch {
      case e: Exception =>
        println("not writing to file")
    }
    finally fw.close()


  }
}
