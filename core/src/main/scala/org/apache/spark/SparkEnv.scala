/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File
import java.net.Socket

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Properties

import akka.actor._
import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.ConnectionManager
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleMemoryManager, ShuffleManager}
import org.apache.spark.storage._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * :: DeveloperApi ::
 *
 * 一个拥有所有spark运行实例(master或者worker)的运行环境对象.
 * 包括:serializer(序列化器),Akka actor system,block manager,map output tracker等等.
 *目前,Spark是通过本地线程池变量来找到SparkEnv的,所以每个线程访问这些对象需要正确的SparkEnv集合.
 * 你可以使用SparkEnv.get(e.g. after creating a SparkContext)得到当前运行环境,并且使用SparkEnv.set.来进行设置
 *
 * 注意:这不意味着你可以随意使用,在未来的版本中,会将其私有化.
 *
 *
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    val actorSystem: ActorSystem,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val securityManager: SecurityManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem,
    val shuffleMemoryManager: ShuffleMemoryManager,
    val conf: SparkConf) extends Logging {

  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  //例如:HadoopFileRDD使用这个方法来缓存JobConfs和InputFormats
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private[spark] def stop() {
    pythonWorkers.foreach { case(key, worker) => worker.stop() }
    Option(httpFileServer).foreach(_.stop())
    mapOutputTracker.stop()
    shuffleManager.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    metricsSystem.stop()
    actorSystem.shutdown()
    //不幸的是,Akka的awaitTermination并不会等待Netty服务的关闭,这个问题需要等到之后的版本进行修正
    // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
    // actorSystem.awaitTermination()
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  private val env = new ThreadLocal[SparkEnv]
  @volatile private var lastSetSparkEnv : SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    lastSetSparkEnv = e
    env.set(e)
  }

  /**
   * 如果非空,返回SparkEnv的本地线程.否则返回在其他线程设置的SparkEnv
   */
  def get: SparkEnv = {
    Option(env.get()).getOrElse(lastSetSparkEnv)
  }

  /**
   * 返回SparkEnv的本地线程
   */
  def getThreadLocal: SparkEnv = {
    env.get()
  }

  private[spark] def create(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean,
      listenerBus: LiveListenerBus = null): SparkEnv = {

    //Listener bus仅用于driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf)
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      actorSystemName, hostname, port, conf, securityManager)


    //列出来Akka实际绑定的端口,防止原始的端口被占用,或者没设置
    //以便于告诉executors正确的连接端口
    if (isDriver) {
      conf.set("spark.driver.port", boundPort.toString)
    }

    //创建一个给定名字的类的实例,可能会使用我们的conf进行初始化
    def instantiateClass[T](className: String): T = {
      val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
      //寻找一个有两个参数的构造函数:SparkConf和boolean SparkConf的
      //另外一个是只有SparkConf
      //还有一个是没有参数
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    //通过给定的SparkConf property或者默认的类名称,来创建一个类的实例
    //如果给定的property没有设置,使用我们的conf来进行初始化.
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    def registerOrLookup(name: String, newActor: => Actor): ActorRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        actorSystem.actorOf(Props(newActor), name = name)
      } else {
        AkkaUtils.makeDriverRef(name, conf, actorSystem)
      }
    }

    val mapOutputTracker =  if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // requires the MapOutputTracker itself
    //在初始化MapOutputTrackerActor后,必须分配trackerActor
    mapOutputTracker.trackerActor = registerOrLookup(
      "MapOutputTracker",
      new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "hash")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    val shuffleMemoryManager = new ShuffleMemoryManager(conf)

    val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
      "BlockManagerMaster",
      new BlockManagerMasterActor(isLocal, conf, listenerBus)), conf)

    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster,
      serializer, conf, securityManager, mapOutputTracker, shuffleManager)

    val connectionManager = blockManager.connectionManager

    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    val cacheManager = new CacheManager(blockManager)

    val httpFileServer =
      if (isDriver) {
        val fileServerPort = conf.getInt("spark.fileserver.port", 0)
        val server = new HttpFileServer(securityManager, fileServerPort)
        server.initialize()
        conf.set("spark.fileserver.uri",  server.serverUri)
        server
      } else {
        null
      }

    val metricsSystem = if (isDriver) {
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      MetricsSystem.createMetricsSystem("executor", conf, securityManager)
    }
    metricsSystem.start()

    //设置sparkFile文件目录,当下载依赖的时候会用.在本地模式下,这是一个临时目录.在分布式模式下,这是executor的工作目录
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir().getAbsolutePath
    } else {
      "."
    }

    // Warn about deprecated spark.cache.class property
    if (conf.contains("spark.cache.class")) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }

    new SparkEnv(
      executorId,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      connectionManager,
      securityManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      shuffleMemoryManager,
      conf)
  }

  /**
   * 返回一个表示JVM信息,Spark属性,系统属性和类路径的map.
   * map的key是定义的目录,map的value是一系列的属性.
   * 主要被SparkListenerEnvironmentUpdate使用
   *
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = System.getProperties.iterator.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
