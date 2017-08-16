package com.verizon.bda.trapezium.dal.solr

import java.io.InputStream

import com.jcraft.jsch.{ChannelExec, JSch, Session}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MMap}
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
    var isRetry = false
    try {
      do {

        var channel: ChannelExec = null

        do {
          if (!isRetry) {
            Thread.sleep(5000)
          }
          channel = session.openChannel("exec").asInstanceOf[ChannelExec]
          channel.setInputStream(null)
          channel.setCommand(command)
          channel.connect
          isRetry = true
        } while (!channel.isConnected || channel == null)
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
  val machineMap: MMap[String, CollectIndices] = new mutable.HashMap[String, CollectIndices]()

  def getMachine(host: String, solrNodeUser: String, solrNodePassword: String): CollectIndices = {
    if (machineMap.contains(host)) {
      machineMap(host)
    } else {
      val scpHost = new CollectIndices
      scpHost.initSession(host, solrNodeUser, solrNodePassword)
      machineMap(host) = scpHost
      scpHost
    }
  }

  def moveFilesFromHdfsToLocal(solrMap: Map[String, String], solrNodeHosts: List[String],
                               indexFilePath: String,
                               movingDirectory: String, coreMap: Map[String, String]): Map[String, ListBuffer[(String, String)]] = {
    log.info("inside move files")
    val solrNodeUser = solrMap("solrUser")
    val solrNodePassword = solrMap("solrNodePassword")
    val solrNodes = new ListBuffer[CollectIndices]

    val shards = coreMap.keySet.toArray
    val sshSequence: Array[(CollectIndices, String, String, String)] = shards.map(shard => {
      log.info(s"shard ${shard}")
      val tmp = shard.split("_")
      val folderPrefix = if (solrMap("folderPrefix").charAt(0) == '/') {
        solrMap("folderPrefix")
      } else {
        "/" + solrMap("folderPrefix")
      }
      val partFile = folderPrefix + (tmp(tmp.length - 2).substring(5).toInt - 1)
      log.info(s"partFile ${partFile}")
      val file = indexFilePath + partFile
      val machine: CollectIndices = getMachine(coreMap(shard).split(":")(0), solrNodeUser, solrNodePassword)
      val command = s"hdfs dfs -copyToLocal $file ${movingDirectory};" +
        s"chmod 777 -R ${movingDirectory};"
      log.info(s"command: ${command}")
      (machine, command, partFile, shard)
    })
    val command = s"mkdir ${movingDirectory}"
    machineMap.values.foreach(_.runCommand(command, false))
    val fileMap = parallelSshFire(sshSequence, movingDirectory)
    machineMap.values.foreach(_.disconnectSession())
    log.info(s"map prepared was " + fileMap.toMap)
    return fileMap.toMap[String, ListBuffer[(String, String)]]
  }

  def parallelSshFire(sshSequence: Array[(CollectIndices, String, String, String)],
                      directory: String): MMap[String, ListBuffer[(String, String)]] = {
    var map = MMap[String, ListBuffer[(String, String)]]()

    val pc1: ParArray[(CollectIndices, String, String, String)] = ParArray.createFromCopy(sshSequence)

    pc1.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    pc1.map(p => {
      p._1.runCommand(p._2, true)
    })
    for ((machine, command, partFile, shard) <- sshSequence) {

      val host = machine.session.getHost
      val fileName = partFile
      if (map.contains(host)) {
        map(host).append((s"${directory}$fileName", shard))
      } else {
        map(host) = new ListBuffer[(String, String)]
        map(host).append((s"${directory}$fileName", shard))
      }
    }
    map
  }

}
