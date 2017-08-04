package com.verizon.bda.trapezium.dal.solr

import java.io.InputStream
import java.net.URI

import com.jcraft.jsch.{ChannelExec, JSch, Session}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.joda.time.LocalDate

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer => MArray, Map => MMap}
import scala.collection.parallel.mutable.ParArray

/**
  * Created by venkatesh on 7/10/17.
  */

class CollectIndices {
  val log = Logger.getLogger(classOf[CollectIndices])
  var session: Session = _

  def initSession(host: String, user: String, password: String) {
    val jsch = new JSch
    session = jsch.getSession(user, host, 22)
    session.setConfig("StrictHostKeyChecking", "no")
    session.setPassword(password)
    session.setTimeout(10000)
    session.connect()
  }

  def disconnectSession() {
    session.disconnect
  }

  def runCommand(command: String, retry: Boolean): Int = {
    var code = -1
    try {
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
        return code
      }
    }
    return code
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

object CollectIndices {
  val log = Logger.getLogger(classOf[CollectIndices])

  def moveFilesFromHdfsToLocal(config: Config): Map[String, ListBuffer[String]] = {
    var map = MMap[String, ListBuffer[String]]()
    //    val map = new ConcurrentHashMap[String, ListBuffer[String]]
    val solrNodeHosts = config.getStringList("solrNodeHosts").asScala
    val solrNodeUser = config.getString("user")
    val solrNodePassword = config.getString("solrNodePassword")
    val solrNodes = new ListBuffer[CollectIndices]
    val localDate = new LocalDate().toString("YYYY_MM_dd")
    val parentDataDir = config.getString("parentDataDir")
    val directory = s"${parentDataDir}_${localDate}"
    for (host <- solrNodeHosts) {
      val scpHost = new CollectIndices
      scpHost.initSession(host, solrNodeUser, solrNodePassword)
      val command = s"mkdir ${directory}"
      scpHost.runCommand(command, false)
      solrNodes.append(scpHost)
    }
    val arr = getHdfsList(config)
    //    val atmic = new AtomicInteger(0)
    var count = 0

    import scala.collection.parallel._
    val pc: Array[(CollectIndices, String, String)] = arr.map(file => {
      val fileName = file.split("/").last
      val machine: CollectIndices = solrNodes(count)


      var command = s"hdfs dfs -copyToLocal $file ${directory};" +
        s"mkdir ${directory}/$fileName/index;" +
        s"mv  ${directory}/$fileName/[^index]*  ${directory}/$fileName/index/.;" +
        s"rm  ${directory}/$fileName/index/*.lock;chmod 777 -R ${directory};"
      count = (count + 1) % solrNodeHosts.size
      (machine, command, fileName)
    })
    val pc1: ParArray[(CollectIndices, String, String)] = mutable.ParArray.createFromCopy(pc)

    pc1.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    pc1.map(p => {
      p._1.runCommand(p._2, true)
    })
    for ((i, j, k) <- pc) {
      val host = i.session.getHost
      val fileName = k
      if (map.contains(host)) {
        map(host).append(s"${directory}/$fileName")
      } else {
        map(host) = new ListBuffer[String]
        map(host).append(s"${directory}/$fileName")
      }
    }

    solrNodes.foreach(_.disconnectSession())
    log.info(s"map prepared was " + map.toMap)
    return map.toMap[String, ListBuffer[String]]
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
