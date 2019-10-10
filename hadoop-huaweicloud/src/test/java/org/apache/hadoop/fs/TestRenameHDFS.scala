package org.apache.hadoop.fs

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer

object TestRenameHDFS {
  def main(args: Array[String]): Unit = {
    val action = args(0).asInstanceOf[String]
    val dir = args(1).asInstanceOf[String]
    val mapNums = Integer.parseInt(args(2).asInstanceOf[String])
    val hadoopPath = args(3).asInstanceOf[String]
    val bucket = args(4).asInstanceOf[String]
    val master = args(5).asInstanceOf[String]
    val conf = new SparkConf()
      .setAppName(action)
    val sc = new SparkContext(conf)
    action match {
      case "list" => {
        println("list")
        val conf = new Configuration()
        val fs = FileSystem.get(URI.create(s"obs://$bucket/"), conf)
        val fStatus: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(dir), true)
        var files = new ListBuffer[String]()
        while (fStatus.hasNext) {
          val status = fStatus.next().asInstanceOf[LocatedFileStatus]
          files += status.getPath.toString.substring(s"obs://$bucket/$dir".length)
        }
        sc.makeRDD(files, mapNums).saveAsTextFile(s"hdfs://$master/$hadoopPath")
      }
      case "rename" => {
        println("rename")
        val parall = Integer.parseInt(args(6).asInstanceOf[String])
        val from = args(7).asInstanceOf[String]
        val to = args(8).asInstanceOf[String]
        val obsBucketBroad = sc.broadcast[String](bucket)
        val hdfsMasterBroad = sc.broadcast[String](master)
        val hdfsPathBroad = sc.broadcast[String](hadoopPath)
        val obsFromBroad = sc.broadcast[String](from)
        val obsToBroad = sc.broadcast[String](to)
        val parallBroad = sc.broadcast[Int](parall)
        val conf = new Configuration()
        val fs = FileSystem.get(URI.create(s"hdfs://$master/$hadoopPath"), conf)
        val paths = fs.listStatus(new Path(s"hdfs://$master/$hadoopPath")).map(sta => {
          sta.getPath.toString
        }).filter(p => !p.contains("SUCCESS"))
        println(s"paths length :${paths.length}")
        val rdd = sc.makeRDD(paths, mapNums)
        rdd.map(hdfsPath => {
          val threadPool: ExecutorService = Executors.newFixedThreadPool(parallBroad.value)
          val conf = new Configuration()
          val obsFs = FileSystem.get(URI.create(s"obs://${obsBucketBroad.value}"), conf)
          val hdfsFs = FileSystem.get(URI.create(s"hdfs://${hdfsMasterBroad.value}/${hdfsPathBroad.value}"), conf)
//          println(s"hdfs path :$hdfsPath")
          var fp : FSDataInputStream = hdfsFs.open(new Path(hdfsPath))
          var isr : InputStreamReader = new InputStreamReader(fp)
          var bReader : BufferedReader = new BufferedReader(isr)
          var line:String = bReader.readLine()
          var futures = new ListBuffer[Future[String]]()
          while (line != null) {
//            println(s"line :$line")
            val sin = line
            val future: Future[String] = threadPool.submit(new Callable[String] {
              override def call(): String = {
                println(s"sin :$sin")
                val fromPathStr = s"${obsFromBroad.value}/$sin"
                val toPathStr = s"${obsToBroad.value}/$sin"
//                println(s"aaa : $fromPathStr - bbb :$toPathStr")
                val fromPath = new Path(fromPathStr)
                val toPath = new Path(toPathStr)
                val begin = System.currentTimeMillis()
                obsFs.rename(fromPath, toPath)
                s"[Thread-${Thread.currentThread().getId}] Rename from path [$fromPathStr] to path [$toPathStr] cost [${System.currentTimeMillis() - begin}] ms."
              }
            })
            futures += future
            line = bReader.readLine()
          }
          var returnStr = ""
          futures.foreach(fu => {
            returnStr += fu.get() + "\n"
          })
          returnStr
        }).saveAsTextFile(s"hdfs://$master/${System.currentTimeMillis()}")
        obsBucketBroad.destroy()
        hdfsMasterBroad.destroy()
        hdfsPathBroad.destroy()
        obsFromBroad.destroy()
        obsToBroad.destroy()
        parallBroad.destroy()
      }
    }
    sc.stop()
  }
}
