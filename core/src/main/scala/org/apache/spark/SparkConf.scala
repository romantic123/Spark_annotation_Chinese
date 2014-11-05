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

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
 *
 *Spark应用程序配置.常常使用k-v对设置Spark参数变量.
 * 很多时候,你可以使用new SparkConf来创建一个SparkConf对象,他将读取各种spark的参数还有你应用程序中有关java的各种设置.
 * 在这种情况下,你可以直接使用SparkConf对象进行设置
 *
 * 在单元测试的时候,你可以使用`new SparkConf(false)`来跳过读取额外的配置和相同配置,不用管这些系统设置什么.
 *
 * 所有的设置方法都支持链式设置,例如,你可以写成这样:
 * `new SparkConf().setMaster("local").setAppName("My app")`.
 *
 * 注意:一旦SparkConf对象传递给Spark,因为传递过去的是副本,所以不能被用户再修改.Spark不支持在Spark运行的时候修改配置
 * 文件.
 * 参数1_loadDefaults:是否获取应用程序中java的设置.
 *
 *
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging {

  import SparkConf._

  /** 创建一个SparkConf对象,设置为true的意思是说,会读取Application中关于java的设置 */
  def this() = this(true)

  private[spark] val settings = new HashMap[String, String]()

  if (loadDefaults) {
    // Load any spark.* system properties
    for ((k, v) <- System.getProperties.asScala if k.startsWith("spark.")) {
      settings(k) = v
    }
  }

  /**  设置一个配置变量 */
  def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }

  /**
   * 设置master的url,可以是local,即本地线程,local[4]代表4个线程,也可以是 "spark://master:7077"这种形式,去运行一个
   * spark standalone集群的方式
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /**设置你app的名字,并且将显示在web ui上*/
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /**  设置jar文件给分布式集群  * */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /**  设置jar文件给分布式集群(java版本) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * 设置一个环境变量,当executors对该application进行执行的时候会用到
   * 这些变量被存储为spark.executorEnv.VAR_NAME的形式(例如spark.executorEnv.PATH),这个方法
   * 在修改各种参数的时候会更容易.
   *
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /**
   * 设置一个环境变量,当executors对该application进行执行的时候会用到
   * 这些变量被存储为spark.executorEnv.VAR_NAME的形式(例如spark.executorEnv.PATH),这个方法
   * 在修改各种参数的时候会更容易.
   *
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /**
   * 设置一个环境变量,当executors对该application进行执行的时候会用到
   * (Java-friendly version.)
   */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /** 设置Spark工作节点的位置*/
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** Set multiple parameters together  设置分布式的参数.... */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /**  如果不存在配置就设置参数 */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (!settings.contains(key)) {
      settings(key) = value
    }
    this
  }

  /** 从配置中删除一个参数 */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }
4
  /**得到一个参数,如果没有设置就抛出一个异常:NoSuchElementException*/
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  /**  得到一个参数,如果没有设置就返回默认*/
  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  /**得到一个参数作以Option的方式*/
  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  /**  得到所有的参数以数组的方式*/
  def getAll: Array[(String, String)] = settings.clone().toArray

  /**  得到一个为Integer形式的参数,没设置就返回默认*/
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** 得到一个为Long形式的参数,没设置就返回默认*/
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** 得到一个为Double形式的参数,没设置就返回默认 */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** 得到一个为Boolean形式的参数,没设置就返回默认 */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** 得到SparkConf对象中,所有executor的环境变量 */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter{case (k, v) => k.startsWith(prefix)}
          .map{case (k, v) => (k.substring(prefix.length), v)}
  }

  /** 得到SparkConf对象中,所有akka的环境变量 */
  def getAkkaConf: Seq[(String, String)] =
    /* This is currently undocumented. If we want to make this public we should consider
     * nesting options under the spark namespace to avoid conflicts with user akka options.
     * Otherwise users configuring their own akka code via system properties could mess up
     * spark's akka options.
     *
     *   E.g. spark.akka.option.x.y.x = "value"
     *
     */
    getAll.filter { case (k, _) => isAkkaConf(k) }

  /**判断配置中是否包含某个参数*/
  def contains(key: String): Boolean = settings.contains(key)

  /** 复制这个对象 */
  override def clone: SparkConf = {
    new SparkConf(false).setAll(settings)
  }

  /**
   * 通过用这个方法,替代System.getenv(),环境变量在单元测试中会被屏蔽
   */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /** 检查非法或者不合理的配置.抛出异常.Not
    * Not idempotent - may mutate this conf object to convert deprecated settings to supported ones.
    * */
  private[spark] def validateSettings() {
    if (settings.contains("spark.local.dir")) {
      val msg = "In Spark 1.0 and later spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
      logWarning(msg)
    }

    val executorOptsKey = "spark.executor.extraJavaOptions"
    val executorClasspathKey = "spark.executor.extraClassPath"
    val driverOptsKey = "spark.driver.extraJavaOptions"
    val driverClassPathKey = "spark.driver.extraClassPath"

    // Validate spark.executor.extraJavaOptions
    settings.get(executorOptsKey).map { javaOpts =>
      if (javaOpts.contains("-Dspark")) {
        val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
          "Set them directly on a SparkConf or in a properties file when using ./bin/spark-submit."
        throw new Exception(msg)
      }
      if (javaOpts.contains("-Xmx") || javaOpts.contains("-Xms")) {
        val msg = s"$executorOptsKey is not allowed to alter memory settings (was '$javaOpts'). " +
          "Use spark.executor.memory instead."
        throw new Exception(msg)
      }
    }

    // Validate memory fractions
    val memoryKeys = Seq(
      "spark.storage.memoryFraction",
      "spark.shuffle.memoryFraction", 
      "spark.shuffle.safetyFraction",
      "spark.storage.unrollFraction",
      "spark.storage.safetyFraction")
    for (key <- memoryKeys) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException("$key should be between 0 and 1 (was '$value').")
      }
    }

    // Check for legacy configs
    sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
      val warning =
        s"""
          |SPARK_JAVA_OPTS was detected (set to '$value').
          |This is deprecated in Spark 1.0+.
          |
          |Please instead use:
          | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
          | - ./spark-submit with --driver-java-options to set -X options for a driver
          | - spark.executor.extraJavaOptions to set -X options for executors
          | - SPARK_DAEMON_JAVA_OPTS to set java options for standalone daemons (master or worker)
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorOptsKey, driverOptsKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }

    sys.env.get("SPARK_CLASSPATH").foreach { value =>
      val warning =
        s"""
          |SPARK_CLASSPATH was detected (set to '$value').
          |This is deprecated in Spark 1.0+.
          |
          |Please instead use:
          | - ./spark-submit with --driver-class-path to augment the driver classpath
          | - spark.executor.extraClassPath to augment the executor classpath
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorClasspathKey, driverClassPathKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_CLASSPATH. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }
  }

  /**
   *返回所有键和值的列表,每行一个.在调bug的时候,非常有用.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}

private[spark] object SparkConf {
  /**
   *判断是否是akka的配置(e.g. akka.actor.provider).
   * 注意:不包括akka的特殊配置.(e.g. spark.akka.timeout).
   * Note that this does not include spark-specific akka configs
   */
  def isAkkaConf(name: String): Boolean = name.startsWith("akka.")

  /**
   *判断是否是executor的启动的配置.
   *当连接到scheduler(调度器)时,driver需要继承一些Spark的配置,此时需要某些akka的配置和认证.
   */
  def isExecutorStartupConf(name: String): Boolean = {
    isAkkaConf(name) ||
    name.startsWith("spark.akka") ||
    name.startsWith("spark.auth") ||
    isSparkPortConf(name)
  }

  /**
   * 判定指定的参数是否是Spark的端口配置
   */
  def isSparkPortConf(name: String): Boolean = name.startsWith("spark.") && name.endsWith(".port")
}
