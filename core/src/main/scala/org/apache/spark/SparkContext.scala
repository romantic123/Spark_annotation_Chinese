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

import scala.language.implicitConversions

import java.io._
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties, UUID}
import java.util.UUID.randomUUID
import scala.collection.{Map, Set}
import scala.collection.JavaConversions._
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.reflect.{ClassTag, classTag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.mesos.MesosNativeLibrary
import akka.actor.Props

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SparkDeploySchedulerBackend, SimrSchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.storage._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{CallSite, ClosureCleaner, MetadataCleaner, MetadataCleanerType, TimeStampedWeakValueHashMap, Utils}

/**
 *
 *   Application创建SparkContext,是Application通往Spark集群的大门.SparkContext可以创建RDD,累加器,广播变量等.
 *   本质上讲:SparkContext是Spark的对外接口,负责向调用方提供各种Spark的功能.作用之一是容器.
 *   参数说明
 *   @param:config:描述application配置的Config对象,例如分配的内存,等等信息.在config对象中设置的属性将覆盖默认配置
 *
 */

class SparkContext(config: SparkConf) extends Logging {

  /**
    目前只在YARN中使用.体现了RDD的第五个特性,计算本地性.
    类型:[hostname,Set[由输入数据的切割信息组成的集合]]
   */
  private[spark] var preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()

  /**
   * 读取系统属性文件信息,创建SparkContext
   */
  def this() = this(new SparkConf())

  /**

   *
   * 在YARN中使用,在Spark创建Executor的时候,选择更本地化(距离更近)的节点作为Executors
   * 其中:
   * 参数2:preferredNodeLocationData,被[[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   * 从应用程序的输入文件集合中生成
   *
   */
  @DeveloperApi
  def this(config: SparkConf, preferredNodeLocationData: Map[String, Set[SplitInfo]]) = {
    this(config)
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  /**
   * 允许直接设置Spark的配置
   * 参数1_master:连接集群的url(e.g. mesos://host:port, spark://host:port, local[4])
   * 参数2_appName:app的名字,可以自己随便起
   * 参数3_SparkConf:[[org.apache.spark.SparkConf]]的对象,用来指定Spark的其他参数设置
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   *允许直接设置Spark的配置
   * 参数1_master:连接集群的url(e.g. mesos://host:port, spark://host:port, local[4])
   * 参数2_appName:app的名字,可以自己随便起
   * 参数3_sparkHome:Spark在集群节点中的位置
   * 参数4_jars:发送给集群的jar序列,其能够通过本地文件系统,HDFS,HTTP,HTTPS,FTP传输
   * 参数5_environment:设置work节点的环境参数
   * 参数6_preferredNodeLocationData:上面讲了
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map(),
      preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) =
  {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  // NOTE: The below constructors could be consolidated using default arguments. Due to
  // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
  // Until we have a good workaround for that bug the constructors remain broken out.

  /**
   *允许直接设置Spark的配置
   * 参数1_master:连接集群的url(e.g. mesos://host:port, spark://host:port, local[4])
   * 参数2_appName:app的名字,可以自己随便起.
   *其实是调用了上面的从构造器
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map(), Map())

  /**
   *允许直接设置Spark的配置
   * 参数1_master:连接集群的url(e.g. mesos://host:port, spark://host:port, local[4])
   * 参数2_appName:app的名字,可以自己随便起
   * 参数3_sparkHome:Spark在集群节点中的位置
   * 其实是调用了上面的从构造器
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map(), Map())

  /**
   *允许直接设置Spark的配置
   * 参数1_master:连接集群的url(e.g. mesos://host:port, spark://host:port, local[4])
   * 参数2_appName:app的名字,可以自己随便起
   * 参数3_sparkHome:Spark在集群节点中的位置
   * 参数4_jars:发送给集群的jar序列,其能够通过本地文件系统,HDFS,HTTP,HTTPS,FTP传输
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map(), Map())

  private[spark] val conf = config.clone()
  conf.validateSettings()

  /**
   * 返回一个Spark的配置文件副本,并且在任务运行时不可修改,用来对运行参数进行判断
   */
  def getConf: SparkConf = conf.clone()

  if (!conf.contains("spark.master")) {
    throw new SparkException("A master URL must be set in your configuration")
  }
  if (!conf.contains("spark.app.name")) {
    throw new SparkException("An application name must be set in your configuration")
  }

  if (conf.getBoolean("spark.logConf", false)) {
    logInfo("Spark configuration:\n" + conf.toDebugString)
  }

  // 设置Driver的host名和端口号
  conf.setIfMissing("spark.driver.host", Utils.localHostName())
  conf.setIfMissing("spark.driver.port", "0")

  val jars: Seq[String] =
    conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val files: Seq[String] =
    conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")


  //为tachyonFolderName的临时文件生成一个随机名字
  //增加一个时间戳确保更安全

  val tachyonFolderName = "spark-" + randomUUID.toString()
  conf.set("spark.tachyonStore.folderName", tachyonFolderName)

  val isLocal = (master == "local" || master.startsWith("local["))

  if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

  //一个异步listeners bus,用以监听Spark的事件
  private[spark] val listenerBus = new LiveListenerBus

  //创建Spark的执行环境(cache(缓存),output tracker(map输出tracker等等))
  private[spark] val env = SparkEnv.create(
    conf,
    "<driver>",
    conf.get("spark.driver.host"),
    conf.get("spark.driver.port").toInt,
    isDriver = true,
    isLocal = isLocal,
    listenerBus = listenerBus)
  SparkEnv.set(env)

  //对每一个文件/jar包,和其时间戳存储到一个k-v对中
  private[spark] val addedFiles = HashMap[String, Long]()
  private[spark] val addedJars = HashMap[String, Long]()

  // 保存RDD转换的过程
  private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
  private[spark] val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, conf)

  // 初始化SparkUI,启动各种监听
  private[spark] val ui = new SparkUI(this)
  ui.bind()

  /** 一个关于Hadoop的默认配置文件(有关文件系统等等)*/
  val hadoopConfiguration: Configuration = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration()
    // Explicitly check for S3 environment variables
    if (System.getenv("AWS_ACCESS_KEY_ID") != null &&
        System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
      hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      hadoopConf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
      hadoopConf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    }
    // Copy any "spark.hadoop.foo=bar" system properties into conf as "foo=bar"
    conf.getAll.foreach { case (key, value) =>
      if (key.startsWith("spark.hadoop.")) {
        hadoopConf.set(key.substring("spark.hadoop.".length), value)
      }
    }
    val bufferSize = conf.get("spark.buffer.size", "65536")
    hadoopConf.set("io.file.buffer.size", bufferSize)
    hadoopConf
  }

  // Spark事件日志
  private[spark] val eventLogger: Option[EventLoggingListener] = {
    if (conf.getBoolean("spark.eventLog.enabled", false)) {
      val logger = new EventLoggingListener(appName, conf, hadoopConfiguration)
      logger.start()
      listenerBus.addListener(logger)
      Some(logger)
    } else None
  }

  //在此时所有的Spark监听程序都已经注册,开始显示事件日志
  listenerBus.start()

  val startTime = System.currentTimeMillis()

  // jars(是driver发送给集群的jar包)对jars中的每个jar,都调用addJar方法
  if (jars != null) {
    jars.foreach(addJar)
  }

  if (files != null) {
    files.foreach(addFile)
  }

  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  private[spark] val executorMemory = conf.getOption("spark.executor.memory")
    .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
    .orElse(Option(System.getenv("SPARK_MEM")).map(warnSparkMem))
    .map(Utils.memoryStringToMb)
    .getOrElse(512)

  //传给executor的环境变量
  private[spark] val executorEnvs = HashMap[String, String]()

  //转换java的option为环境变量
  //不能够通过sbt直接设置
  for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
    value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
    executorEnvs(envKey) = value
  }
  Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
    executorEnvs("SPARK_PREPEND_CLASSES") = v
  }


  //Mesos Scheduler backend依赖于这个环境变量设置executor的内存
  // TODO: Set this only in the Mesos schedule.
  executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
  executorEnvs ++= conf.getExecutorEnv

  //用SPARK_USER为正在运行的SparkContext设置user名
  val sparkUser = Option {
    Option(System.getenv("SPARK_USER")).getOrElse(System.getProperty("user.name"))
  }.getOrElse {
    SparkContext.SPARK_UNKNOWN_USER
  }
  executorEnvs("SPARK_USER") = sparkUser

  // 创建和开始一个Scheduler
  private[spark] var taskScheduler = SparkContext.createTaskScheduler(this, master)
  private val heartbeatReceiver = env.actorSystem.actorOf(
    Props(new HeartbeatReceiver(taskScheduler)), "HeartbeatReceiver")
  @volatile private[spark] var dagScheduler: DAGScheduler = _
  try {
    dagScheduler = new DAGScheduler(this)
  } catch {
    case e: Exception => throw
      new SparkException("DAGScheduler cannot be initialized due to %s".format(e.getMessage))
  }

  // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
  // constructor
  taskScheduler.start()

  private[spark] val cleaner: Option[ContextCleaner] = {
    if (conf.getBoolean("spark.cleaner.referenceTracking", true)) {
      Some(new ContextCleaner(this))
    } else {
      None
    }
  }
  cleaner.foreach(_.start())

  postEnvironmentUpdate()
  postApplicationStart()

  private[spark] var checkpointDir: Option[String] = None

  //本地线程变量能被用户用来传递信息
  private val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = new Properties(parent)
  }

  private[spark] def getLocalProperties: Properties = localProperties.get()

  private[spark] def setLocalProperties(props: Properties) {
    localProperties.set(props)
  }

  @deprecated("Properties no longer need to be explicitly initialized.", "1.0.0")
  def initLocalProperties() {
    localProperties.set(new Properties())
  }

  /**
   * 设置本地属性,会对该线程的作业提交产生影响,例如 Spark fair scheduler pool.
   *
   */
  def setLocalProperty(key: String, value: String) {
    if (localProperties.get() == null) {
      localProperties.set(new Properties())
    }
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
   * 得到这个线程的本地属性设置,如果没有设置为null,见[[org.apache.spark.SparkContext.setLocalProperty]].
   */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).getOrElse(null)

  /**
    *设置一个可读的job描述
    * */

  @deprecated("use setJobGroup", "0.8.1")
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**

   *对该线程的所有job分配group id,直到这个group被设置为不同值或被清除
   *很多时候,应用程序中一个代码块的执行包括很多Spark的action或者job.应用程序的开发者能用这个方法对所有的job进行分组,
   * 并且设置分组描述,一旦设置,Spark 的Web UI将对这些jobs进行分组
   *
   * 该应用程序也能够用[[org.apache.spark.SparkContext.cancelJobGroup]]去取消所有这个group的job,例如
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * 如果interruptOnCancel在job group中设置为true,在job的executor上job cancellation将导致线程中断.
   * 这是有助于确保任务能够及时停止.但是由于在HDFS-1208的默认情况下,可能会认为线程中断是节点挂了.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
    // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
    // APIs to also take advantage of this property (e.g., internal job failures or canceling from
    // JobProgressTab UI) on a per-job basis.
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /**清空当前线程job的group id和他的描述*/
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  // Post init
  taskScheduler.postStartHook()

  private val dagSchedulerSource = new DAGSchedulerSource(this.dagScheduler, this) //维护以用以度量dag各个指标的对象
  private val blockManagerSource = new BlockManagerSource(SparkEnv.get.blockManager, this)

  private def initDriverMetrics() {
    SparkEnv.get.metricsSystem.registerSource(dagSchedulerSource)
    SparkEnv.get.metricsSystem.registerSource(blockManagerSource)
  }

  initDriverMetrics()

  // Methods for creating RDDs

  /**
   *将一个RDD进行切片
   *
   * 参数1_seq:是一个不可变集合,并且在调用parallelize之后和第一次action之前是可变的.修改之后的RDD反映出
   * 对集合的修改.
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
    *将一个RDD进行切片
    * 参数1_seq:是一个不可变集合,并且在调用parallelize之后和第一次action之前是可变的.修改之后的RDD反映出
    * 对集合的修改.
   */
  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    parallelize(seq, numSlices)
  }

  /**
    * 将一个RDD进行切片,但是对于每个对象有更多的参数选择(Spark节点的hostnames)
    * 对每个集合创建一个新的Partition
    * */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = {
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this, seq.map(_._1), seq.size, indexToPrefs)
  }

  /***
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * 从HDFS,本地文件系统读取text文件,返回一个RDD
   */
  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /**
   *
   * 从HDFS,本地文件系统读取一个text文件的目录,每个文件都会被读,并且返回一个k-v对,k是路径,v是文件内容,很常用的功能
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *]
   * 注意:最好小文件,大文件会影响性能
   * 参数1_minPartitions:对输入数据的最小切割数,至于多少你看着定
   *
   */
  def wholeTextFiles(path: String, minPartitions: Int = defaultMinPartitions):
  RDD[(String, String)] = {
    val job = new NewHadoopJob(hadoopConfiguration)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this,
      classOf[WholeTextFileInputFormat],
      classOf[String],
      classOf[String],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * 从一个Hadoop JobConf(e.g. file name for a filesystem-based dataset, table name for HyperTable)描述的Hadoop-readable只读数据集中,获取一个RDD
   * 参数1_conf:数据集的配置/描述文件
   * 参数2_inputFormatClass:输入格式的类
   * 参数3_keyClass:key的类
   * 参数4_valueClass:values的类
   * 参数5_minPartitions:hadoop切片的最小数量
   *
   *
   * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
   * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
      ): RDD[(K, V)] = {
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *得到一个Hadoop文件,输入格式随意
    * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
    * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
    * */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions
      ): RDD[(K, V)] = {
    //一个hadoop的配置文件大约10kb,对于再大的,就要broadcast他了
    val confBroadcast = broadcast(new SerializableWritable(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   *使用类表标签来指出keys的class,值和输入数据的格式,以便user不能直接跳过,相反调用者只能写
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
   * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   *使用类表标签来指出keys的class,值和输入数据的格式,以便user不能直接跳过,相反调用者只能写
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
   * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] =
    hadoopFile[K, V, F](path, defaultMinPartitions)

  /**
    * 从一个具有任意新API输入格式的Hadoop文件处得到一个RDD
   *    * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
   * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
   * */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   *得到一个给定Hadoop文件的RDD,并且抽取输入文件的配置参数
   *    * 注意:因为Hadoop的RecordReader类会对每个record重复使用相同的Writable对象,直接缓存返回的RDD将对相同对象创建许多references,
   * 如果逆向直接缓存hadoop 的writable对象,你应该用map方法复制他们....
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
    val job = new NewHadoopJob(conf)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * 和上面一样
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = {
    new NewHadoopRDD(this, fClass, kClass, vClass, conf)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    *得到一个Hadoop Sequence(给定了key和value类型)文件的RDD
    * 注意:.....和上面一样
    */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types.
    *得到一个Hadoop Sequence(给定了key和value类型)文件的RDD
    * 注意:.....和上面一样
    */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]
      ): RDD[(K, V)] =
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)

  /**
   *这个版的sequenceFile可以将类型通过WritableConverter隐式转换为Writables,例如,访问key为Text,值为IntWritable
   * 的SequenceFile文件,你可以这样写:
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   *
   * WritableConverters提供了一些特殊的方式(隐式转换)支持Writable的子类和我们定义的一个转换(converter)(比如,int转换IntWritable)
   *更多的时候,我们已经有了converters的隐式对象,但是却没有Writable的每个子类(即没有一个参数化的单例对象).
   * 我们使用方法代替创建一个适当类型的converter.此外，我们通过转换器的classtag以便找出使用子类中的情况下，可写的类类型。
   *
   * 注意:和上面一样
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V])
      : RDD[(K, V)] = {
    val kc = kcf()
    val vc = vcf()
    val format = classOf[SequenceFileInputFormat[Writable, Writable]]
    val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
    writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
  }

  /**
   *
   * 载入一个RDD,保存为一个序列化对象的SequenceFile文件,该序列化对象包含一系列partition的NullWritable类型的key
   * 和BytesWritable类型的值的值
   *
   *
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions
      ): RDD[T] = {
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  protected[spark] def checkpointFile[T: ClassTag](
      path: String
    ): RDD[T] = {
    new CheckpointRDD[T](this, path)
  }

  /**   构建RDDs集合*/
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = new UnionRDD(this, rdds)

  /** 构建RDDs的集合,通过variable-length参数 */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
    new UnionRDD(this, Seq(first) ++ rest)

  /** 得到没有partitions或者elements的RDD
    * */
  def emptyRDD[T: ClassTag] = new EmptyRDD[T](this)

  // Methods for creating shared variables

  /**
   * 创建一个给定类型的变量,其只能够通过"+="来增加值,只有driver能够使用累加器的值
   */
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  /**
   * 创建一个给定类型的变量,你可以给他起个名字,以便在Spark UI中显示,其只能够通过"+="来增加值,只有driver能够使用累加器的值
   */
  def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]) = {
    new Accumulator(initialValue, param, Some(name))
  }

  /**
   * 创建一个给定类型的变量,其只能够通过"+="来增加值,只有driver能够使用累加器的值
   * 参数1_T:累加器的类型
   * 参数2_R:累加器增加的类型(type that can be added to the accumulator)
   */
  def accumulable[T, R](initialValue: T)(implicit param: AccumulableParam[T, R]) =
    new Accumulable(initialValue, param)

  /**
   * 创建一个给定类型的变量,你可以给他起个名字,以便在Spark UI中显示,其只能够通过"+="来增加值,只有driver能够使用累加器的值
   * 参数1_T:累加器的类型
   * 参数2_R:累加器增加的类型(type that can be added to the accumulator)
   */
  def accumulable[T, R](initialValue: T, name: String)(implicit param: AccumulableParam[T, R]) =
    new Accumulable(initialValue, param, Some(name))

  /**
   *创建一个"可变集合(mutable collection)"类型的累加器
   *   +=和++=是Growable和TraversableOnce的标准API,被标准不可变集合实现,比如Map,Set等
   */
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
      (initialValue: R): Accumulable[R, T] = {
    val param = new GrowableAccumulableParam[R,T]
    new Accumulable(initialValue, param)
  }

  /**.
   * Broadcast对于集群来说是一个只读的变量,在distributed functions读取他时,
   * 返回一个[[org.apache.spark.broadcast.Broadcast]]对象.该变量将被只一次被发送到集群
   * 广播变量
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
    cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   *增加一个文件被Spark每个工作节点下载.可以通过本地文件系统,HDFS,或者HTTP,HTTPS,或者FTP,Spark的job使用
   * `SparkFiles.get(path)`来找到文件的下载路径.
   */
  def addFile(path: String) {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addFile(new File(uri.getPath))
      case "local"       => "file:" + uri.getPath
      case _             => path
    }
    addedFiles(key) = System.currentTimeMillis

    // Fetch the file locally in case a job is executed using DAGScheduler.runLocally().
    Utils.fetchFile(path, new File(SparkFiles.getRootDirectory()), conf, env.securityManager)

    logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
    postEnvironmentUpdate()
  }

  /**
   * :: DeveloperApi ::
   * 注册一个监听器,接收执行期间发生的返回事件.
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  /** 这个application正在运行在哪个版本的Spark上*/
  def version = SparkContext.SPARK_VERSION

  /**
   * 返回slave节点的缓存的最大可用内存和剩余可用内存缓存的map
   *
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * 返回关于RDDs的缓存信息,比如他们在内存或者磁盘上需要多少空间等信息
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    val rddInfos = persistentRdds.values.map(RDDInfo.fromRdd).toArray
    StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
    rddInfos.filter(_.isCached)
  }

  /**
   *返回已经物化缓存的RDDs的不可变的map集合.
   * 注意:这并不意味着缓存一定成功
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: DeveloperApi ::
   *返回所有slave上关于blocks的信息
   */
  @DeveloperApi
  def getExecutorStorageStatus: Array[StorageStatus] = {
    env.blockManager.master.getStorageStatus
  }

  /**
   * :: DeveloperApi ::
   * 返回所有调度程序pools
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    // TODO(xiajunluan): We should take nested pools into account
    //我们应
    taskScheduler.rootPool.schedulableQueue.toSeq
  }

  /**
   * :: DeveloperApi ::
   * 返回给定名字的pool,如果其存在
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /**
   * 返回当前的调度模式
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    taskScheduler.schedulingMode
  }

  /**
   * 清理掉使用addFile这个方法增加的files集合,以便其他新节点不能下载
   */
  @deprecated("adding files no longer creates local copies that need to be deleted", "1.0.0")
  def clearFiles() {
    addedFiles.clear()
  }

  /**
   * 得到一个RDD中partition相关的位置信息
   * 参数1_rdd:你指定的RDD
   * 参数2_partition:要查找的地方
   * 返回:对于partition优先位置列表
   *
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /**
   * 注册一个RDD,并将其持久化到内存或者磁盘存储
   */
  private[spark] def persistRDD(rdd: RDD[_]) {
    persistentRdds(rdd.id) = rdd
  }

  /**
   * 从内存或者磁盘解除RDD的持久化
   */
  private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
    env.blockManager.master.removeRdd(rddId, blocking)
    persistentRdds.remove(rddId)
    listenerBus.post(SparkListenerUnpersistRDD(rddId))
  }

  /**
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   * 增加一个jar依赖为在SparkContext上将被执行的所有任务.路径可以是,本地文件系统,HDFS,HTTP,HTTPS,FTP,或者
   * 每个工作节点的local:/path上的文件
   *
   */
  def addJar(path: String) {
    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      var key = ""
      if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        key = env.httpFileServer.addJar(new File(path))
      } else {
        val uri = new URI(path)
        key = uri.getScheme match {
          // A JAR file which exists only on the driver node
          case null | "file" =>
            // yarn-standalone is deprecated, but still supported
            if (SparkHadoopUtil.get.isYarnMode() &&
                (master == "yarn-standalone" || master == "yarn-cluster")) {
              // In order for this to work in yarn-cluster mode the user must specify the
              // --addJars option to the client to upload the file into the distributed cache
              // of the AM to make it show up in the current working directory.
              val fileName = new Path(uri.getPath).getName()
              try {
                env.httpFileServer.addJar(new File(fileName))
              } catch {
                case e: Exception =>
                  // For now just log an error but allow to go through so spark examples work.
                  // The spark examples don't really need the jar distributed since its also
                  // the app jar.
                  logError("Error adding jar (" + e + "), was the --addJars option used?")
                  null
              }
            } else {
              env.httpFileServer.addJar(new File(uri.getPath))
            }
          // A JAR file which exists locally on every worker node
          case "local" =>
            "file:" + uri.getPath
          case _ =>
            path
        }
      }
      if (key != null) {
        addedJars(key) = System.currentTimeMillis
        logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
      }
    }
    postEnvironmentUpdate()
  }

  /**
   * 清除掉通过addJar方法添加的jar包,以使其他节点不能够下载啦.
   */
  @deprecated("adding jars no longer creates local copies that need to be deleted", "1.0.0")
  def clearJars() {
    addedJars.clear()
  }

  /** 关闭SparkContext*/
  def stop() {
    postApplicationEnd()
    ui.stop()
    //如果仍不能停止,就会报错.
    //如果停止了不止一次,防止NPE
    val dagSchedulerCopy = dagScheduler
    dagScheduler = null
    if (dagSchedulerCopy != null) {
      env.metricsSystem.report()
      metadataCleaner.cancel()
      env.actorSystem.stop(heartbeatReceiver)
      cleaner.foreach(_.stop())
      dagSchedulerCopy.stop()
      taskScheduler = null
      // TODO: Cache.stop()?
      env.stop()
      SparkEnv.set(null)
      listenerBus.stop()
      eventLogger.foreach(_.stop())
      logInfo("Successfully stopped SparkContext")
    } else {
      logInfo("SparkContext already stopped")
    }
  }


  /**
   * 无论是通过构造函数,还是spark.home,或者Spark_Home的环境变量(按照这个构造顺序),如果这些位置都没有
   * 返回为None.
   *
   */
  private[spark] def getSparkHome(): Option[String] = {
    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
  }

  /**
   * API的backtraces支持方法
   */
  def setCallSite(site: String) {
    setLocalProperty("externalCallSite", site)
  }

  /**
   * API的backtraces支持方法
   */
  def clearCallSite() {
    setLocalProperty("externalCallSite", null)
  }

  /**
   * 获取当前用的callsite,并且返回一个格式化的version.如果用户覆写了callsite,将返回用户的版本
   */
  private[spark] def getCallSite(): CallSite = {
    Option(getLocalProperty("externalCallSite")) match {
      case Some(callSite) => CallSite(callSite, longForm = "")
      case None => Utils.getCallSite
    }
  }

  /**
   * 在给定的RDD的partitions集合上运行一个方法,并将结果传给处理函数,这个是Spark的主要方法.allowLocal标志
   * 指定调度程序能否在driver上运行计算,而不是交给集群进行计算.很多不复杂的功能,例如first()方法
   *
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) {
    if (dagScheduler == null) {
      throw new SparkException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    val start = System.nanoTime
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
      resultHandler, localProperties.get)
    logInfo(
      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
    rdd.doCheckpoint()
  }

  /**
   * 在给定的RDD的partitions集合上运行一个方法,返回一个结果数组,这个是Spark的主要方法.allowLocal标志
   * 指定调度程序能否在driver上运行计算,而不是交给集群进行计算.很多不复杂的功能,例如first()方法
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
    results
  }

  /**
   * 在给定的RDD的partitions集合上运行一个方法,但是方法类型Iterator[T] => U代替了上面的(TaskContext, Iterator[T]) => U
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions, allowLocal)
  }

  /**
   * 在给定的RDD的partitions集合上运行一个方法,并且返回一个结果array
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   *在给定的RDD的partitions集合上运行一个方法,并且返回一个结果array
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   *
   * 在给定的RDD的partitions集合上运行一个方法,将结果传递到一个处理函数
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   *  在给定的RDD的partitions集合上运行一个方法,将结果传递到一个处理函数
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   * :: DeveloperApi ::
   * 运行一个job,返回近似的结果
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    val callSite = getCallSite
    logInfo("Starting job: " + callSite.shortForm)
    val start = System.nanoTime
    val result = dagScheduler.runApproximateJob(rdd, func, evaluator, callSite, timeout,
      localProperties.get)
    logInfo(
      "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * :: Experimental ::
   * 提交一个job执行,并且将结果作为一个FutureJob返回
   * */
  @Experimental
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    val cleanF = clean(processPartition)
    val callSite = getCallSite
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      allowLocal = false,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * 取消指定分组的active jobs,参见See [[org.apache.spark.SparkContext.setJobGroup]]了解更多
   */
  def cancelJobGroup(groupId: String) {
    dagScheduler.cancelJobGroup(groupId)
  }

  /**取消所有已经被调度或者运行的job
    * */
  def cancelAllJobs() {
    dagScheduler.cancelAllJobs()
  }

  /** 取消指定的job运行或者调度,就将其取消*/
  private[spark] def cancelJob(jobId: Int) {
    dagScheduler.cancelJob(jobId)
  }

  /**
    * 取消指定stage的所有job
    * */
  private[spark] def cancelStage(stageId: Int) {
    dagScheduler.cancelStage(stageId)
  }

  /**
   * 清理一个闭包,使之能够序列化和可以发送任务(删除引用变量外,更新REPL变量).如果checkSerializable设置,clean
   * 将会主动检查,如果f是可序列化的,将会抛出一个SparkExceptiobn的异常
   *
   * 参数1_f:要清理的闭包
   * 参数2_checkSerializable:设置是否立即检查f是否是可序列化的
   * throws:如果checkSerializable设置立即检查f是否是可序列化的,且f不可序列化,就会抛出SparkException
   *
   */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * 设置RDD的checkpointed的目录,这个目录一定是在集群中运行的HDFS路径
   */
  def setCheckpointDir(directory: String) {
    checkpointDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString)
      val fs = path.getFileSystem(hadoopConfiguration)
      fs.mkdirs(path)
      fs.getFileStatus(path).getPath.toString
    }
  }

  def getCheckpointDir = checkpointDir

  /** 用户没有自己设置时候,默认的parallelism级别(e.g. parallelize and makeRDD) */
  def defaultParallelism: Int = taskScheduler.defaultParallelism

  /**   用户没有对Hadoop RDD指定partitions个数的时候,默认的是2
    * */
  @deprecated("use defaultMinPartitions", "1.0.0")
  def defaultMinSplits: Int = math.min(defaultParallelism, 2)

  /** 用户没有对Hadoop RDD指定partitions个数的时候,默认的是2*/
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /**  注册一个新的RDD,返回他的RDD的id*/
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**  提交应用程序的开始事件*/
  private def postApplicationStart() {
    listenerBus.post(SparkListenerApplicationStart(appName, startTime, sparkUser))
  }

  /**  提交应用程序的开始事件*/
  private def postApplicationEnd() {
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
  }

  /** 一旦任务调度系统准备就绪就更新环境信息*/
  private def postEnvironmentUpdate() {
    if (taskScheduler != null) {
      val schedulingMode = getSchedulingMode.toString
      val addedJarPaths = addedJars.keys.toSeq
      val addedFilePaths = addedFiles.keys.toSeq
      val environmentDetails =
        SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths, addedFilePaths)
      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
      listenerBus.post(environmentUpdate)
    }
  }

  /**定时清理persistentRdds map
    * */
  private[spark] def cleanup(cleanupTime: Long) {
    persistentRdds.clearOldValues(cleanupTime)
  }
}

/**
 * 该SparkContext对象包括Spark的特征中要用的各种隐式转换和参数
 */
object SparkContext extends Logging {

  private[spark] val SPARK_VERSION = "1.1.0"

  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"

  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"

  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"

  private[spark] val SPARK_UNKNOWN_USER = "<unknown>"

  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double) = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int) = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long) = t1 + t2
    def zero(initialValue: Long) = 0L
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float) = t1 + t2
    def zero(initialValue: Float) = 0f
  }

  // TODO: Add AccumulatorParams for other types, e.g. lists and strings

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]) = new AsyncRDDActions(rdd)

  implicit def rddToSequenceFileRDDFunctions[K <% Writable: ClassTag, V <% Writable: ClassTag](
      rdd: RDD[(K, V)]) =
    new SequenceFileRDDFunctions(rdd)

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](
      rdd: RDD[(K, V)]) =
    new OrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]) = new DoubleRDDFunctions(rdd)

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]) =
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))

  // Implicit conversions to common Writable types, for saveAsSequenceFile
  //常见的Writable类型转换
  implicit def intToIntWritable(i: Int) = new IntWritable(i)

  implicit def longToLongWritable(l: Long) = new LongWritable(l)

  implicit def floatToFloatWritable(f: Float) = new FloatWritable(f)

  implicit def doubleToDoubleWritable(d: Double) = new DoubleWritable(d)

  implicit def boolToBoolWritable (b: Boolean) = new BooleanWritable(b)

  implicit def bytesToBytesWritable (aob: Array[Byte]) = new BytesWritable(aob)

  implicit def stringToText(s: String) = new Text(s)

  private implicit def arrayToArrayWritable[T <% Writable: ClassTag](arr: Traversable[T])
    : ArrayWritable = {
    def anyToWritable[U <% Writable](u: U): Writable = u

    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  // Helper objects for converting common types to Writable
  //帮助转换的方法对象
  private def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T)
      : WritableConverter[T] = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  implicit def intWritableConverter(): WritableConverter[Int] =
    simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter(): WritableConverter[Long] =
    simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter(): WritableConverter[Double] =
    simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter(): WritableConverter[Float] =
    simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter(): WritableConverter[Boolean] =
    simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
    simpleWritableConverter[Array[Byte], BytesWritable](_.getBytes)
  }

  implicit def stringWritableConverter(): WritableConverter[String] =
    simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable]() =
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])

  /**
   *找到一个指定的jar从一个给定的类,通过SparkContext获取他们的jars
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * 查找包含特定对象的jar,方便用户可以很容易的使用SparkContext调用他们的jars.在大多数情况下,你可以
   * 在你的driver应用程序中调用jarOfObject
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /**
   * 创建一个修改后的SparkConf,比SparkContext的构造函数更容易修改.无效值会忽略,而不会像SparkConf一样抛出一个异常
   */
  private[spark] def updatedConf(
      conf: SparkConf,
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()): SparkConf =
  {
    val res = conf.clone()
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /** * 根据给定的master url创建一个task scheduler.提取测试.
    * */
  private def createTaskScheduler(sc: SparkContext, master: String): TaskScheduler = {
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """spark://(.*)""".r
    // Regular expression for connection to Mesos cluster by mesos:// or zk:// url
    val MESOS_REGEX = """(mesos|zk)://.*""".r
    // Regular expression for connection to Simr cluster
    val SIMR_REGEX = """simr://(.*)""".r

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(scheduler, 1)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_N_REGEX(threads) =>
        def localCpuCount = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalBackend(scheduler, threadCount)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        def localCpuCount = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        // local[N, M] means exactly N threads with M failures
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalBackend(scheduler, threadCount)
        scheduler.initialize(backend)
        scheduler

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (sc.executorMemory > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, sc.executorMemory))
        }

        val scheduler = new TaskSchedulerImpl(sc)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt)
        val masterUrls = localCluster.start()
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
          localCluster.stop()
        }
        scheduler

      case "yarn-standalone" | "yarn-cluster" =>
        if (master == "yarn-standalone") {
          logWarning(
            "\"yarn-standalone\" is deprecated as of Spark 1.0. Use \"yarn-cluster\" instead.")
        }
        val scheduler = try {
          val clazz = Class.forName("org.apache.spark.scheduler.cluster.YarnClusterScheduler")
          val cons = clazz.getConstructor(classOf[SparkContext])
          cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]
        } catch {
          // TODO: Enumerate the exact reasons why it can fail
          // But irrespective of it, it means we cannot proceed !
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }
        val backend = try {
          val clazz =
            Class.forName("org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend")
          val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
          cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }
        scheduler.initialize(backend)
        scheduler

      case "yarn-client" =>
        val scheduler = try {
          val clazz =
            Class.forName("org.apache.spark.scheduler.cluster.YarnClientClusterScheduler")
          val cons = clazz.getConstructor(classOf[SparkContext])
          cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]

        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }

        val backend = try {
          val clazz =
            Class.forName("org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend")
          val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
          cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
        } catch {
          case e: Exception => {
            throw new SparkException("YARN mode not available ?", e)
          }
        }

        scheduler.initialize(backend)
        scheduler

      case mesosUrl @ MESOS_REGEX(_) =>
        MesosNativeLibrary.load()
        val scheduler = new TaskSchedulerImpl(sc)
        val coarseGrained = sc.conf.getBoolean("spark.mesos.coarse", false)
        val url = mesosUrl.stripPrefix("mesos://") // strip scheme from raw Mesos URLs
        val backend = if (coarseGrained) {
          new CoarseMesosSchedulerBackend(scheduler, sc, url)
        } else {
          new MesosSchedulerBackend(scheduler, sc, url)
        }
        scheduler.initialize(backend)
        scheduler

      case SIMR_REGEX(simrUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val backend = new SimrSchedulerBackend(scheduler, sc, simrUrl)
        scheduler.initialize(backend)
        scheduler

      case _ =>
        throw new SparkException("Could not parse Master URL: '" + master + "'")
    }
  }
}

/**
 *  *实现将typeT转换为Writable.他存储了和Writable和与Writable类型一致的类型T (e.g. IntWritable for Int)和一个转换的方法
 *writable 类的getter方法
 * The getter for the writable class takes a ClassTag[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
 * 听上去很棒,但是很多时候我们不必转换Writable的子类.
 *

 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

