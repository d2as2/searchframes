package com.verizon.bda.trapezium.dal.solr

import java.io.InputStream
import java.net.URI

import com.jcraft.jsch.{ChannelExec, JSch, Session}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger

import scala.collection.mutable.{ListBuffer, ArrayBuffer => MArray, Map => MMap}
import scala.collection.parallel.ForkJoinTaskSupport
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
    log.info(s"making ssh session with ${user}@${host}")
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

  def moveFilesFromHdfsToLocal(solrMap: Map[String, String], solrNodeHosts: List[String],
                               indexFilePath: String,
                               movingDirectory: String): Map[String, ListBuffer[String]] = {
    val solrNodes1 = solrNodeHosts
    val solrNodeUser = solrMap("solrUser")
    val solrNodePassword = solrMap("solrNodePassword")
    val solrNodes = new ListBuffer[CollectIndices]
    for (host <- solrNodes1) {
      val scpHost = new CollectIndices
      scpHost.initSession(host, solrNodeUser, solrNodePassword)
      val command = s"mkdir ${movingDirectory}"
      scpHost.runCommand(command, false)
      solrNodes.append(scpHost)
    }
    val arr = getHdfsList(solrMap, indexFilePath)
    //    val atmic = new AtomicInteger(0)
    var count = 0
    val sshSequence: Array[(CollectIndices, String, String)] = arr.map(file => {
      val fileName = file.split("/").last
      val machine: CollectIndices = solrNodes(count)


      var command = s"hdfs dfs -copyToLocal $file ${movingDirectory};" +
        s"mkdir ${movingDirectory}/$fileName/index;" +
        s"mv  ${movingDirectory}/$fileName/[^index]*  ${movingDirectory}/$fileName/index/.;" +
        s"rm  ${movingDirectory}/$fileName/index/*.lock;chmod 777 -R ${movingDirectory};"
      count = (count + 1) % solrNodeHosts.size
      (machine, command, fileName)
    })

    val fileMap = parallelSshFire(sshSequence, movingDirectory)
    solrNodes.foreach(_.disconnectSession())
    log.info(s"map prepared was " + fileMap.toMap)
    return fileMap.toMap[String, ListBuffer[String]]
  }

  def parallelSshFire(sshSequence: Array[(CollectIndices, String, String)],
                      directory: String): MMap[String, ListBuffer[String]] = {
    var map = MMap[String, ListBuffer[String]]()

    val pc1: ParArray[(CollectIndices, String, String)] = ParArray.createFromCopy(sshSequence)

    pc1.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    pc1.map(p => {
      p._1.runCommand(p._2, true)
    })
    for ((i, j, k) <- sshSequence) {
      val host = i.session.getHost
      val fileName = k
      if (map.contains(host)) {
        map(host).append(s"${directory}/$fileName")
      } else {
        map(host) = new ListBuffer[String]
        map(host).append(s"${directory}/$fileName")
      }
    }
    map
  }

  def getHdfsList(solrMap: Map[String, String], indexFilePath: String): Array[String] = {
    val configuration: Configuration = new Configuration()
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    // 2. Get the instance of the HDFS
    val nameNaode = solrMap("nameNode")
    // + config.getString("indexFilesPath")
    // config.getString("hdfs")
    val hdfs = FileSystem.get(new URI(s"hdfs://${nameNaode}"), configuration)
    // 3. Get the metadata of the desired directory
    val fileStatus = hdfs.listStatus(new Path(s"hdfs://${nameNaode}" + indexFilePath))
    // 4. Using FileUtil, getting the Paths for all the FileStatus
    val paths = FileUtil.stat2Paths(fileStatus)
    val folderPrefix = solrMap("folderPrefix")
    paths.map(_.toString).filter(p => p.contains(folderPrefix))
  }
}
