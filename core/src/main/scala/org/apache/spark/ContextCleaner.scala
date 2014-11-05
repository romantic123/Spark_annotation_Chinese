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

import java.lang.ref.{ReferenceQueue, WeakReference}

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Classes that represent cleaning tasks.
 * 代表Clean任务的类
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask

/**
 *
 * 一个清理任务的弱引用
 *
 * 当参照对象成为弱引用时,相应的CleanupTaskWeakReference会被自动添加到给定参考队列
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 *
 * RDD,shuffle,broadcast,state的异步clear类
 *
 * 当相关的对象超出application的范围的时候,这能保持RDD的弱引用ShffleDependency,感兴趣对的广播变量能被处理
 *  在一个守护进程执行实际的cleanup操作
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  private val referenceBuffer = new ArrayBuffer[CleanupTaskWeakReference]
    with SynchronizedBuffer[CleanupTaskWeakReference]

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val listeners = new ArrayBuffer[CleanerListener]
    with SynchronizedBuffer[CleanerListener]

  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  /**
   * 是否清理线程将这个块的task清理掉(除了shuffle,使用`spark.cleaner.referenceTracking.blocking.shuffle`来
   * 控制)
   *
   * 在Spark-3015中,其设置是默认的.这是一个工作环境的问题,最终导致 BlockManager actors相互依赖,以至于akka之间的
   * 消息传输过于频繁.当这一切发生的时候,对于实例,则driver会执行GC和清理所有不再范围内的广播变量.
   */
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  /**
   *
   * 是否清理线程将shuffle任务清理掉
   *
   *当在每个删除请求中配置context cleaner时,将会抛出超时异常.避免这个问题,这个参数默认禁用shuffle clean操作.
   * 注意这个操作不会影响广播变量和RDDs.这事一个临时解决方案,没办法,上面的`blockOnCleanupTasks`也是解决方案之一.
   *
   */
  private val blockOnShuffleCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking.shuffle", false)

  @volatile private var stopped = false

  /** 当对象被清理的时候设置一个监听对象,去得到清理的信息*/
  def attachListener(listener: CleanerListener) {
    listeners += listener
  }

  /** 开始清理 */
  def start() {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
  }

  /** 开始清理 */
  def stop() {
    stopped = true
  }

  /**当垃圾收集的时候,为cleanup注册一个RDD
    * */
  def registerRDDForCleanup(rdd: RDD[_]) {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  /**  当垃圾收集的时候,为cleanup注册一个ShuffleDependency
    * */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]) {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /**  当垃圾收集的时候,为cleanup注册一个广播变量 */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]) {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** 为cleanup注册一个对象*/
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask) {
    referenceBuffer += new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue)
  }

  /**保持RDD清理,shuffle,和广播变量的状态*/
  private def keepCleaning(): Unit = Utils.logUncaughtExceptions {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        reference.map(_.task).foreach { task =>
          logDebug("Got cleaning task " + task)
          referenceBuffer -= reference.get
          task match {
            case CleanRDD(rddId) =>
              doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
            case CleanShuffle(shuffleId) =>
              doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
            case CleanBroadcast(broadcastId) =>
              doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
          }
        }
      } catch {
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** 执行RDD的清理 */
  def doCleanupRDD(rddId: Int, blocking: Boolean) {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /**执行shuffle清理,异步的  */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean) {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /**执行广播变量的清理 */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean) {
    try {
      logDebug("Cleaning broadcast " + broadcastId)
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.foreach(_.broadcastCleaned(broadcastId))
      logInfo("Cleaned broadcast " + broadcastId)
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * 用于测试item是否已经被Cleaner类清理掉的Listener类
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int)
  def shuffleCleaned(shuffleId: Int)
  def broadcastCleaned(broadcastId: Long)
}
