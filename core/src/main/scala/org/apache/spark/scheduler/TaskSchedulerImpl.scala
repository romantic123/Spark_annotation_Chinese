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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{TimerTask, Timer}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import akka.actor.Props

/**
 *
 * 在集群中各种任务调度都是通过SchedulerBackend来完成的.
 * 通过设置isLocal为true,也能够在本地通过调用LocalBackend进行任务调度.
 * 他可以处理常见的工作,比如调度job的次序,唤醒执行推测式任务(speculative tasks)等等
 *
 * client端应该首先调用initialize()和start()方法,然后提交任务集合给runTasks()方法
 *
 * SAchedulerBackends和任务提交客户端都能够从多个线程中调用这个类,所以需要将其锁在公共API中维持他的状态.
 * 此外,当他们想发送events的时候,需要SchedulerBackends同步他们自己,也就是锁定自己,所以我们必须保证,当我们在锁定自己状态
 * 的时候,不能锁定这个backend.
 *
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  //检查推测式任务的频率
  val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

  //超过时间阈值,将会警告用户初始化的TaskSet可能starved(饥饿--得不到调度)
  val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

  //每个任务需要的CPUs
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  //TaskSetManager不是线程安全的,所以任何访问都应该和这个类同步
  val activeTaskSets = new HashMap[String, TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // 增加任务ID
  val nextTaskId = new AtomicLong(0)

  //我们已经执行的executors的id
  val activeExecutorIds = new HashSet[String]

  //每个主机上的executors的集合,这被用来计算hosts是否活着,用来决定我们什么时候获取给定host上的本地data
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  //监听对象传递到回调的地方
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  //默认的调度是FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  //这是一个var,以便我们能够重新设置他达到测试的目的
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    //暂时设置rootPool的名字为空
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      import sc.env.actorSystem.dispatcher
      sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
            SPECULATION_INTERVAL milliseconds) {
        Utils.tryOrExit { checkSpeculatableTasks() }
      }
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = new TaskSetManager(this, taskSet, maxTaskFailures)
      activeTaskSets(taskSet.id) = manager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient memory")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
      //有两种情况
      //1.创建任务管理器,并且一些任务已经被调度 .在这点上,发送一个要杀掉的signal给executors去杀掉相应的任务,然后终止stage
      //2.创建任务管理器,但是没有任务调度呢.此时,简单的终止这个stage
      tsm.runningTasksSet.foreach { tid =>
        val execId = taskIdToExecutorId(tid)
        backend.killTask(tid, execId, interruptThread)
      }
      tsm.abort("Stage %s cancelled".format(stageId))
      logInfo("Stage %d was cancelled".format(stageId))
    }
  }

  /**
   *
   * 根据给定的TaskSetManager,调用指出所有的task(包括推测式任务)都已经完成.所以和TaskSetManager有关的stage都应该
   * 删除
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    activeTaskSets -= manager.taskSet.id
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }

  /**
   * 在slaves中被cluster manager调用来申请资源.我们根据任务的优先级来进行相应.我们采用一种round-robin的方式来
   * 分发我们的任务,以便使集群中的任务平衡.
   *
   *
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    SparkEnv.set(sc.env)

    // 使每个节点是激活的,且记住他的hostname
    //如果新的executor增加了,也要跟踪
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // 随机洗牌避免总是将同一个tasks放进相同的workers
    val shuffledOffers = Random.shuffle(offers)
    //为每个worker都建立一个task集合
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    //按照调度器的顺序take每个TaskSet,然后对每个node都增加一个locality级别,以便在执行本地任务时,能够在本地上执行
    //而不需要其他节点来执行.
    //注意:执行顺序: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        launchedTask = false
        for (i <- 0 until shuffledOffers.size) {
          val execId = shuffledOffers(i).executorId
          val host = shuffledOffers(i).host
          if (availableCpus(i) >= CPUS_PER_TASK) {
            for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
              tasks(i) += task
              val tid = task.taskId
              taskIdToTaskSetId(tid) = taskSet.taskSet.id
              taskIdToExecutorId(tid) = execId
              activeExecutorIds += execId
              executorsByHost(host) += execId
              availableCpus(i) -= CPUS_PER_TASK
              assert(availableCpus(i) >= 0)
              launchedTask = true
            }
          }
        }
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              taskIdToExecutorId.remove(tid)
            }
            activeTaskSets.get(taskSetId).foreach { taskSet =>
              if (state == TaskState.FINISHED) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
               "likely the result of receiving duplicate task finished status updates)")
              .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**
   * 更新正在运行task的指标,并且告诉master这个BlockManager仍然活着.如果driver直到给定的BlockManager就返回true.
   * 否则,返回false,表明block manager应该重新注册.
   *
   */
  override def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
      blockManagerId: BlockManagerId): Boolean = {

    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) =>
        taskIdToTaskSetId.get(id)
          .flatMap(activeTaskSets.get)
          .map(taskSetMgr => (id, taskSetMgr.stageId, taskSetMgr.taskSet.attempt, metrics))
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long) {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]) = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: TaskEndReason) = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      //需要再次接收offers,该任务设置manager状态已经被更新了,失败的任务需要重新运行.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (activeTaskSets.size > 0) {
        //每个任务错误的时候抛出一个SparkException异常
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        //没有任务的状态是活跃的,但是我们仍有一个错误.这意味着在注册阶段,必须会有一个错误发生,所以我们仅仅退出就行.
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()

    //线程休息1秒钟,并确定信息已经发送出去了.
    Thread.sleep(1000L)
  }

  override def defaultParallelism() = backend.defaultParallelism()

  //在我们所有的还活着的job中检查推测式任务(speculateable)
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {

        //我们可能因为多种不同的失败原因,多次调用executorLost().例如,当其中一个executor终止的时候,会导致
        // 另一个和slave之间的连接断开.我们为两个都生成log信息,报告终止原因.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    //dagScheduler.executorLost没有锁定,以防止死锁.
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**  从我们的数据结构中删除一个executor,并且标记为lost * */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      synchronized {
        this.wait(100)
      }
    }
  }
}


private[spark] object TaskSchedulerImpl {
  /**
   *
   * 用来平衡hosts的容器.
   *
   * 接受一个(host,host申请的资源)类型的map,返回一个资源优先级列表.申请的资源是有序的,我们给每个host都分配一个容器,
   * 在其他host上分配第二个容器,等等,为了减少因为host失败造成的损失.
   *
   * 例如:
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   *
   *
   *
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    //keyList的顺序基于map的值
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        //如果存在,得到host的索引
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
