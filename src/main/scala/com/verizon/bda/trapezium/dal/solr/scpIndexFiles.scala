package com.verizon.bda.trapezium.dal.solr

import java.io.{File, InputStream}
import java.net.URI

import com.jcraft.jsch.{ChannelExec, JSch, Session}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger


/**
  * Created by venkatesh on 7/10/17.
  */

class scpIndexFiles {
  val log = Logger.getLogger(classOf[scpIndexFiles])
  var session: Session = _

  def uploadSolr(config: Config): Unit = {
  }


  def initSession(config: Config) {
    val jsch = new JSch
    val user = config.getString("user")
    val host = config.getString("host")
    val password = config.getString("password")
    session = jsch.getSession(user, host, 22)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setPassword(password)
    session.connect()
  }

  def disconnectSession() {
    session.disconnect
  }

  def runCommand(command: String) {
    try {
      val channel: ChannelExec = session.openChannel("exec").asInstanceOf[ChannelExec]
      channel.setInputStream(null)
      channel.setCommand(command)
      channel.connect
      val in: InputStream = channel.getInputStream
      printResult(in, channel)
      channel.disconnect()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def printResult(in: InputStream, channel: ChannelExec): Unit = {
    val tmp = new Array[Byte](1024)
    var continueLoop = true
    while (continueLoop) {
      while (in.available > 0) {
        val i = in.read(tmp, 0, 1024)
        if (i < 0) continueLoop = false
        log.info(new String(tmp, 0, i))
      }
      if (continueLoop && channel.isClosed) {
        log.warn("exit-status:" + channel.getExitStatus)
        continueLoop = false
      }
    }
  }

  def getHdfsList(config: Config): Array[String] = {
    val configuration: Configuration = new Configuration()
    // 2. Get the instance of the HDFS
    val hdfs = FileSystem.get(new URI("hdfs:///"), configuration)
    // 3. Get the metadata of the desired directory
    val indexFilesPath = config.getString("indexFilesPath")
    val fileStatus = hdfs.listStatus(new Path("hdfs:///" + indexFilesPath))
    // 4. Using FileUtil, getting the Paths for all the FileStatus
    val paths = FileUtil.stat2Paths(fileStatus)
    val fileList = paths.map(_.toString).filter(_.contains("2017"))
    fileList
  }


}

object scpIndexFiles {
  def main(args: Array[String]): Unit = {
    val sci = new scpIndexFiles()
    val files = List("/user/poosave/weather/2016_Weather_Data.csv",
      "/user/poosave/weather/2017_3_1~3_7_Weather_Data.csv",
      "/user/poosave/weather/2017_Jan~Feb_Weather_Data_Final.csv")
    // sci.runCommand(command)
    sci.initSession(ConfigFactory.parseFile(new File("/Users/poosave/Documents/git/" +
      "trapezium/dal/src/main/resources/solrLucen.conf")))
    for (file <- files) {
      val command = s"hdfs dfs -copyToLocal $file /data0/poosave/test/"
      sci.runCommand(command)
    }
    sci.disconnectSession()

  }
}