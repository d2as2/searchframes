package com.verizon.bda.trapezium.dal.solr

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.net.URI
import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.jcraft.jsch.{ChannelExec, JSch, Session}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.joda.time.LocalDate

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
  * Created by venkatesh on 7/10/17.
  */

class ScpIndexFiles {
  val log = Logger.getLogger(classOf[ScpIndexFiles])
  var session: Session = _

  def initSession(host: String, user: String, password: String) {
    val jsch = new JSch
    session = jsch.getSession(user, host, 22)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setPassword(password)
    session.connect()
  }

  def disconnectSession() {
    session.disconnect
  }

  def runCommand(command: String, retry: Boolean) {
    try {

      var code = -1
      do {
        val channel: ChannelExec = session.openChannel("exec").asInstanceOf[ChannelExec]
        channel.setInputStream(null)
        channel.setCommand(command)
        channel.connect
        log.info(s"running command : ${command} in ${session.getHost}" +
          s" with user ${session.getUserName}")
        val in: InputStream = channel.getInputStream
        code = printResult(in, channel)

        channel.disconnect()
      } while (retry && code != 0)

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def printResult(in: InputStream, channel: ChannelExec): Int = {
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
    return channel.getExitStatus
  }


}

object ScpIndexFiles {
  def moveFilesFromHdfsToLocal(config: Config): Unit = {
    val solrNodeHosts = config.getStringList("solrNodeHosts").asScala
    val solrNodeUser = config.getString("user")
    val solrNodePassword = config.getString("solrNodePassword")
    val lib = new ListBuffer[ScpIndexFiles]
    val localDate = new LocalDate().toString("YYYY_MM_DD")
    val dataDir = config.getString("dataDir")
    val directory = s"${dataDir}_${localDate}"
    for (host <- solrNodeHosts) {
      val scpHost = new ScpIndexFiles
      scpHost.initSession(host, solrNodeUser, solrNodePassword)
      val command = s"mkdir ${directory}"
      scpHost.runCommand(command, false)
      lib.append(scpHost)
    }
    val arr = getHdfsList(config)
    //    val atmic = new AtomicInteger(0)
    var count = 0
    // todo make multi threaded
    for (file <- arr) {
      val command = s"hdfs dfs -copyToLocal $file ${directory}"
      lib(count).runCommand(command, true)
      count = (count + 1) % solrNodeHosts.size
    }
    lib.foreach(_.disconnectSession())
  }

  def getHdfsList(config: Config): Array[String] = {
    val configuration: Configuration = new Configuration()
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    // 2. Get the instance of the HDFS
    val nameNaode = config.getString("nameNode")
    // + config.getString("indexFilesPath")
    // config.getString("hdfs")
    val hdfs = FileSystem.get(new URI(s"hdfs://${nameNaode}"), configuration)
    // 3. Get the metadata of the desired directory
    val indexFilesPath = config.getString("indexFilesPath")
    val fileStatus = hdfs.listStatus(new Path(s"hdfs://${nameNaode}" + indexFilesPath))
    // 4. Using FileUtil, getting the Paths for all the FileStatus
    val paths = FileUtil.stat2Paths(fileStatus)
    val folderPrefix = config.getString("folderPrefix")
    paths.map(_.toString).filter(p => p.contains(folderPrefix))
  }
}
