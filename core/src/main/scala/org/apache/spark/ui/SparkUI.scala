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

package org.apache.spark.ui

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentTab
import org.apache.spark.ui.exec.ExecutorsTab
import org.apache.spark.ui.jobs.JobProgressTab
import org.apache.spark.ui.storage.StorageTab

/**
 * Spark应用程序的最顶级接口
 *
 */
private[spark] class SparkUI(
    val sc: SparkContext,
    val conf: SparkConf,
    val securityManager: SecurityManager,
    val listenerBus: SparkListenerBus,
    var appName: String,
    val basePath: String = "")
  extends WebUI(securityManager, SparkUI.getUIPort(conf), conf, basePath, "SparkUI")
  with Logging {

  def this(sc: SparkContext) = this(sc, sc.conf, sc.env.securityManager, sc.listenerBus, sc.appName)
  def this(conf: SparkConf, listenerBus: SparkListenerBus, appName: String, basePath: String) =
    this(null, conf, new SecurityManager(conf), listenerBus, appName, basePath)

  def this(
      conf: SparkConf,
      securityManager: SecurityManager,
      listenerBus: SparkListenerBus,
      appName: String,
      basePath: String) =
    this(null, conf, securityManager, listenerBus, appName, basePath)

  //如果SparkContext没有提供,那就认定程序没有启动
  val live = sc != null

  //通过Spark的事件,保持executor的的storage状态
  val storageStatusListener = new StorageStatusListener

  initialize()

  /** 初始化Server的所有组件 */
  def initialize() {
    listenerBus.addListener(storageStatusListener)
    val jobProgressTab = new JobProgressTab(this)
    attachTab(jobProgressTab)
    attachTab(new StorageTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/", "/stages", basePath = basePath))
    attachHandler(
      createRedirectHandler("/stages/stage/kill", "/stages", jobProgressTab.handleKillRequest))
    if (live) {
      sc.env.metricsSystem.getServletHandlers.foreach(attachHandler)
    }
  }

  def getAppName = appName

  /** 设置在UI上显示的app名字 */
  def setAppName(name: String) {
    appName = name
  }

  /** 注册listener bus.指定的监听器
    * */
  def registerListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  /**停止server的web接口,直到在bind()方法执行的时候. */
  override def stop() {
    super.stop()
    logInfo("Stopped Spark web UI at %s".format(appUIAddress))
  }

  /**返回app的UI host:post.   这包括scheme(http://)
   */
  private[spark] def appUIHostPort = publicHostName + ":" + boundPort

  private[spark] def appUIAddress = s"http://$appUIHostPort"
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.getAppName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }
}
