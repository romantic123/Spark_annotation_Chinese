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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map, Stack}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.pattern.ask
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.util.{CallSite, SystemClock, Clock, Utils}
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat

/**
 *
 *stage-oriented调度的最高层次的调度层.他对每个job都计算了一个DAG的stage,跟踪那些RDD和Stage,并将输出物化,
 * 然后找到一个最小的schedule运行这个job.然后将stage作为taskSets提交给TaskSchedulerImpl,去在集群中运行.
 *
 * 此外,划分DAG的stage,计算运行这个task的最佳节点,考虑当前的缓存状态,都是通过low-level的TaskScheduler.
 * 此外,shuffle后的文件丢失,他将根据需要的stage重新计算,提交.不是shuffle文件丢失导致的失败,都将交给TaskScheduler
 * 来处理,在取消整个stage之前,还会进行一些次数的重试.
 *
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = SystemClock)
  extends Logging {

  import DAGScheduler._

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  private[scheduler] val shuffleToMapStage = new HashMap[Int, Stage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  //将要发送给taskSchedulder的stage
  private[scheduler] val waitingStages = new HashSet[Stage]

  //正在运行stage
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  //由于失败,stage必须重新提交
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  //包含每个RDD的partitions的缓存位置
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]

  /** 跟踪失败的节点,我们使用MapOutputTracker的epoch number,来发送每个任务.当发现节点失败的时候,我们记录当前的
    * epoch number和失败的executor,重开一个新的task,并且用这个忽略stray ShuffleMapTask的结果
    * */
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private val dagSchedulerActorSupervisor =
    env.actorSystem.actorOf(Props(new DAGSchedulerActorSupervisor(this)))

  //我门重用的一个封闭序列化器
  //这仅仅是因为DAGScheduler运行在一个单独线程更加安全
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  private[scheduler] var eventProcessActor: ActorRef = _

  /** 如果启用,我们可能在本地运行一些actionn,像:take()和first() */
  private val localExecutionEnabled = sc.getConf.getBoolean("spark.localExecution.enabled", false)

  private def initializeEventProcessActor() {
    //阻塞线程直到supervisor开始,并且要确保在job提交之前eventProcessActor非空


    implicit val timeout = Timeout(30 seconds)
    val initEventActorReply =
      dagSchedulerActorSupervisor ? Props(new DAGSchedulerEventProcessActor(this))
    eventProcessActor = Await.result(initEventActorReply, timeout.duration).
      asInstanceOf[ActorRef]
  }

  initializeEventProcessActor()

  //TaskScheduler报告说任务开始了
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessActor ! BeginEvent(task, taskInfo)
  }

  //获取task完成且结果被远程读取的报告
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessActor ! GettingResultEvent(taskInfo)
  }

  //TaskScheduler报告程序完成还是失败
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics) {
    eventProcessActor ! CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics)
  }

  /**
   * 对正在运行的task更新metrics,并且让master知道BlockManager仍然活着,如果driver知道给定的block manager还活着
   * 就返回true.否则就返回false.表明blockManager应该重新注册.
   */
  def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    implicit val timeout = Timeout(600 seconds)

    Await.result(
      blockManagerMaster.driverActor ? BlockManagerHeartbeat(blockManagerId),
      timeout.duration).asInstanceOf[Boolean]
  }

  //当executor失败的时候被TaskScheduler调用
  def executorLost(execId: String) {
    eventProcessActor ! ExecutorLost(execId)
  }

  //当一个host被增加的时候,被TaskScheduler调用
  def executorAdded(execId: String, host: String) {
    eventProcessActor ! ExecutorAdded(execId, host)
  }

  //TaskScheduler调用,用来取消一些总失败的TaskSet,或者Job
  def taskSetFailed(taskSet: TaskSet, reason: String) {
    eventProcessActor ! TaskSetFailed(taskSet, reason)
  }

  private def getCacheLocs(rdd: RDD[_]): Array[Seq[TaskLocation]] = {
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
      val locs = BlockManager.blockIdsToBlockManagers(blockIds, env, blockManagerMaster)
      cacheLocs(rdd.id) = blockIds.map { id =>
        locs.getOrElse(id, Nil).map(bm => TaskLocation(bm.host, bm.executorId))
      }
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs() {
    cacheLocs.clear()
  }

  /**
   *
   * 对给定的shuffle依赖的map side,得到或者创建一个shuffle map stage. JobID的值将被使用,如过stage不存在
   * 或者jobid太小(jobid的值是增加的)
   */
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, jobId)
        // Then register current shuffleDep
        val stage =
          newOrUsedStage(
            shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
            shuffleDep.rdd.creationSite)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
 
        stage
    }
  }

  /**
   *
   * 创建一个stage---无论是作为结果stage直接使用,还是重新创建shuffle map stage.这个stage将关联到你提供的
   * jobID,shuffle map stages应该直接用于newOrUsedStage,而不是直接newStage
   */
  private def newStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val id = nextStageId.getAndIncrement()
    val stage =
      new Stage(id, rdd, numTasks, shuffleDep, getParentStages(rdd, jobId), jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * 为给定的RDD创建一个shuffle map stage.这个stage将和提供的jobid关联.
   *
   * 如果一个shuffleID的stage在当前的MapOutputTracker中已经存在了,那么就从MapOutPutTracker可用的outpus路径中恢复
   */
  private def newOrUsedStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) {
        stage.outputLocs(i) = Option(locs(i)).toList   // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)
    }
    stage
  }

  /**
   *
   * 对一个给定的RDD得到或者创建一个父stage集合.如果其在lower的JobID中不存在,那么这个stage将和提供的jobId匹配起来
   */
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }

  //找到一个没有任何shuffle依赖的ancestor,然后注册shuffleToMapStage
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], jobId: Int) = {
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (!parentsWithNoMapStage.isEmpty) {
      val currentShufDep = parentsWithNoMapStage.pop()
      val stage =
        newOrUsedStage(
          currentShufDep.rdd, currentShufDep.rdd.partitions.size, currentShufDep, jobId,
          currentShufDep.rdd.creationSite)
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }

  //找到一个有shuffle依赖的ancestor,且还没有注册的shuffleToMapStage
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    //手动维护一个堆栈,来避免由于递归访问导致的栈溢出
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   *
   * 在所有stage的ancestors之中,为需要的stage,注册给定的JobID
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage) {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * 删除job的stage和任何job都不会使用到的stage.不处理取消的tasks或者sparkListener通知已经结束的jobs/stages/tasks.
   *
   * 参数1_job:要清理的stage的job
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob) {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId

              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
   * 提交给job Scheduler一个job并且返回一个JobWaiter对象.这个JobWaiter对象能够被用于阻塞,直到job执行完或者被用来
   * 取消这个job
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null): JobWaiter[U] =
  {
    // Check to make sure we are not launching a task on a partition that does not exist.
    //检查以确保我们
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessActor ! JobSubmitted(
      jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter, properties)
    waiter
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
  {
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded => {}
      case JobFailed(exception: Exception) =>
        logInfo("Failed to run " + callSite.shortForm)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties = null)
    : PartialResult[R] =
  {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.size).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessActor ! JobSubmitted(
      jobId, rdd, func2, partitions, allowLocal = false, callSite, listener, properties)
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * 取消队列中正在运行或者等待的job
   */
  def cancelJob(jobId: Int) {
    logInfo("Asked to cancel job " + jobId)
    eventProcessActor ! JobCancelled(jobId)
  }

  def cancelJobGroup(groupId: String) {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessActor ! JobGroupCancelled(groupId)
  }

  /**
   * 取消队列中所有正在等待或者运行的job
   */
  def cancelAllJobs() {
    eventProcessActor ! AllJobsCancelled
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.jobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * 取消所有和给定stage相关联的jobs
   */
  def cancelStage(stageId: Int) {
    eventProcessActor ! StageCancelled(stageId)
  }

  /**
   * 提交任何失败的stage.且距离上次失败的时间不会太短...
   *
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      //失败的stage可能会被job cancellation给删除掉,所以失败可能会被清空,即便你后面已将让其执行ResubmitFailedStages
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.jobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
   * 检查等待或者失败的stages,现在重新提交.
   * 一般在eventloop中的每次迭代中运行
   *
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    for (stage <- waitingStagesCopy.sortBy(_.jobId)) {
      submitStage(stage)
    }
  }

  /**
   *在本地RDD中运行一个Job,假设只有一个partition,且没有依赖.
   *我们将其运行在一个单线程中,防止其会花费很多时间,所以在此我们不能阻塞DAGScheduler的event loop(事件轮询)还有其他jobs
   */
  protected def runLocally(job: ActiveJob) {
    logInfo("Computing the requested partition locally")
    new Thread("Local computation of job " + job.jobId) {
      override def run() {
        runLocallyWithinThread(job)
      }
    }.start()
  }

  //在DAGSchedulerSuite中更容易测试
  protected def runLocallyWithinThread(job: ActiveJob) {
    var jobResult: JobResult = JobSucceeded
    try {
      SparkEnv.set(env)
      val rdd = job.finalStage.rdd
      val split = rdd.partitions(job.partitions(0))
      val taskContext =
        new TaskContext(job.finalStage.id, job.partitions(0), 0, runningLocally = true)
      try {
        val result = job.func(taskContext, rdd.iterator(split, taskContext))
        job.listener.taskSucceeded(0, result)
      } finally {
        taskContext.markTaskCompleted()
      }
    } catch {
      case e: Exception =>
        val exception = new SparkDriverExecutionException(e)
        jobResult = JobFailed(exception)
        job.listener.jobFailed(exception)
      case oom: OutOfMemoryError =>
        val exception = new SparkException("Local job aborted due to out of memory error", oom)
        jobResult = JobFailed(exception)
        job.listener.jobFailed(exception)
    } finally {
      //清理一个本地job中的数据结构,但是当事件完成或者stage支持,不能清理
      //
      stageIdToStage -= s.id
      jobIdToStageIds -= job.jobId
      listenerBus.post(SparkListenerJobEnd(job.jobId, jobResult))
    }
  }

  /** 在stage找到一个更容易创建的job
    * */
  // TODO: Probably should actually find among the active jobs that need this
  //stage中优先级最高的(最早创建的,highest-priority pool)
  //应该关注交叉作业的优先级问题,还有cross job依赖问题
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    //取消这个job分组的所有job
    //找到这个group id中所有的活跃job,然后关掉他们
    val activeInGroup = activeJobs.filter(activeJob =>
      groupId == activeJob.properties.get(SparkContext.SPARK_JOB_GROUP_ID))
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    //注意,在stage取消之后,task是有机会启动的
    //在这点上,我们在stageIdToStage这个阶段将不会再有stage
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(taskSet: TaskSet, reason: String) {
    stageIdToStage.get(taskSet.stageId).foreach {abortStage(_, reason) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      runningStages.foreach { stage =>
        stage.latestInfo.stageFailed(stageFailedMessage)
        listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties = null)
  {
    var finalStage: Stage = null
    try {
      //一个新stage划分出来,可能会抛出一个异常,例如,在运行在HadoopRDD的job会由于HDFS文件被删除而抛出异常
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions (allowLocal=%s)".format(
        job.jobId, callSite.shortForm, partitions.length, allowLocal))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      val shouldRunLocally =
        localExecutionEnabled && allowLocal && finalStage.parents.isEmpty && partitions.length == 1
      if (shouldRunLocally) {
        //一些很简单的actions,类似first()和task()等不依赖父stage的action可以运行在本地
        listenerBus.post(SparkListenerJobStart(job.jobId, Array[Int](), properties))
        runLocally(job)
      } else {
        jobIdToActiveJob(jobId) = job
        activeJobs += job
        finalStage.resultOfJob = Some(job)
        listenerBus.post(SparkListenerJobStart(job.jobId, jobIdToStageIds(jobId).toArray,
          properties))
        submitStage(finalStage)
      }
    }
    submitWaitingStages()
  }

  /**  提交stage,但是会对没有父依赖的进行递归提交
    * */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing == Nil) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }

  /** 当stage的父依赖是可用,且我们要提交依赖的父stage时调用
    * */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = {
      if (stage.isShuffleMap) {
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id) == Nil)
      } else {
        val job = stage.resultOfJob.get
        (0 until job.numPartitions).filter(id => !job.finished(id))
      }
    }

    val properties = if (jobIdToActiveJob.contains(jobId)) {
      jobIdToActiveJob(stage.jobId).properties
    } else {
      //这个stage将被默认的pool关联
      null
    }

    runningStages += stage
    //SparkListenerStageSubmitted会测试要提交的任务是否是可序列化的.如果不可序列化,SparkListenerStageCompleted
    //事件将会被提交,紧接着是SparkListenerStageSubmitted事件
    stage.latestInfo = StageInfo.fromStage(stage, Some(partitionsToCompute.size))
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    //广播二进制的任务,用于分发任务给executor.注意广播的是RDD的副本并且要将每个task进行反序列化.这意味每个task都会
    //获得不同的RDD副本.这提供了更强的隔离,对集群中可能修改对象参照状态的那些任务.在Hadoop中,这事非常必要的,
    //因为JobConf/Configuration对象不是线程安全的
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] =
        if (stage.isShuffleMap) {
          closureSerializer.serialize((stage.rdd, stage.shuffleDep.get) : AnyRef).array()
        } else {
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func) : AnyRef).array()
        }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString)
        runningStages -= stage
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {
      partitionsToCompute.map { id =>
        val locs = getPreferredLocs(stage.rdd, id)
        val part = stage.rdd.partitions(id)
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
      }
    } else {
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
    }

    if (tasks.size > 0) {
      //预先序列化任务,以确保器可以序列化.在这里我们将捕捉异常,因为相比非序列化异常,他更难捕捉,我们在本地调度和集群
      //调度上有不同的实现
      //我们已经在taskBinary中序列化了RDD和closeurs,但是在这我们检查所有对象和Partition
      try {
        closureSerializer.serialize(tasks.head)
      } catch {
        case e: NotSerializableException =>
          abortStage(stage, "Task not serializable: " + e.toString)
          runningStages -= stage
          return
        case NonFatal(e) => // Other exceptions, such as IllegalArgumentException from Kryo.
          abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
          runningStages -= stage
          return
      }

      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)
      taskScheduler.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTime())
    } else {
      //因为我们之前了提交SparkListenerStageSubmitted,我们应该在此处提交SparkListenerStageCompleted以防止没有
      //任务在运行
      listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
      logDebug("Stage " + stage + " is actually done; %b %d %d".format(
        stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
      runningStages -= stage
    }
  }

  /**
   * 相应一个任务结束.这就是所谓的event Loop(事件轮询),他假定可以修改调度程序的内部状态.使用taskEnded()来
   * 发布一个task 结束的event.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    // The success case is dealt with separately below, since we need to compute accumulator
    // updates before posting.
    //
    if (event.reason != Success) {
      val attemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
      listenerBus.post(SparkListenerTaskEnd(stageId, attemptId, taskType, event.reason,
        event.taskInfo, event.taskMetrics))
    }

    if (!stageIdToStage.contains(task.stageId)) {
      //如果stage取消了,就跳过所有的actions
      return
    }
    val stage = stageIdToStage(task.stageId)

    def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None) = {
      val serviceTime = stage.latestInfo.submissionTime match {
        case Some(t) => "%.03f".format((clock.getTime() - t) / 1000.0)
        case _ => "Unknown"
      }
      if (errorMessage.isEmpty) {
        logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
        stage.latestInfo.completionTime = Some(clock.getTime())
      } else {
        stage.latestInfo.stageFailed(errorMessage.get)
        logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
      }
      listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
      runningStages -= stage
    }
    event.reason match {
      case Success =>
        if (event.accumUpdates != null) {
          try {
            Accumulators.add(event.accumUpdates)
            event.accumUpdates.foreach { case (id, partialValue) =>
              val acc = Accumulators.originals(id).asInstanceOf[Accumulable[Any, Any]]
              // To avoid UI cruft, ignore cases where value wasn't updated
              if (acc.name.isDefined && partialValue != acc.zero) {
                val name = acc.name.get
                val stringPartialValue = Accumulators.stringifyPartialValue(partialValue)
                val stringValue = Accumulators.stringifyValue(acc.value)
                stage.latestInfo.accumulables(id) = AccumulableInfo(id, name, stringValue)
                event.taskInfo.accumulables +=
                  AccumulableInfo(id, name, Some(stringPartialValue), stringValue)
              }
            }
          } catch {
            //如果我们看到一个累加器更新的异常,就记录错误,继续运行
            case e: Exception =>
              logError(s"Failed to update accumulators for $task", e)
          }
        }
        listenerBus.post(SparkListenerTaskEnd(stageId, stage.latestInfo.attemptId, taskType,
          event.reason, event.taskInfo, event.taskMetrics))
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            stage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(stage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(SparkListenerJobEnd(job.jobId, JobSucceeded))
                  }

                  //taskSucceeded运行用户的代码可能会导致异常.确保我们可以从容应对
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the stage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo("Ignoring possibly bogus ShuffleMapTask completion from " + execId)
            } else {
              stage.addOutputLoc(smt.partitionId, status)
            }
            if (runningStages.contains(stage) && stage.pendingTasks.isEmpty) {
              markStageAsFinished(stage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)
              if (stage.shuffleDep.isDefined) {
                // We supply true to increment the epoch number here in case this is a
                // recomputation of the map outputs. In that case, some nodes may have cached
                // locations with holes (from when we detected the error) and will need the
                // epoch incremented to refetch them.
                // TODO: Only increment the epoch number if this is not the first time
                //   注册map outputs
                mapOutputTracker.registerMapOutputs(
                  stage.shuffleDep.get.shuffleId,
                  stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
                  changeEpoch = true)
              }
              clearCacheLocs()
              if (stage.outputLocs.exists(_ == Nil)) {
                //一些任务失败了,重新提交这个stage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + stage + " (" + stage.name +
                  ") because some of its tasks had failed: " +
                  stage.outputLocs.zipWithIndex.filter(_._1 == Nil).map(_._2).mkString(", "))
                submitStage(stage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                for (stage <- waitingStages) {
                  logInfo("Missing parents for " + stage + ": " + getMissingParentStages(stage))
                }
                for (stage <- waitingStages if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  stage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(stage)
                } {
                  logInfo("Submitting " + stage + " (" + stage.rdd + "), which is now runnable")
                  submitMissingTasks(stage, jobId)
                }
              }
            }
          }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingTasks += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)
        //很可能我们从一个stage接收多个FetchFailed(因为我们有多个任务运行在不同的executor上).在这点上,可能有些获取的
        //失败已经由调度程序处理了
        if (runningStages.contains(failedStage)) {
          logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
            s"due to a fetch failure from $mapStage (${mapStage.name})")
          markStageAsFinished(failedStage, Some("Fetch failure"))
          runningStages -= failedStage
        }

        if (failedStages.isEmpty && eventProcessActor != null) {
          //如果失败信息是非空,不要重新对失败的stages进行调度了,因为其已经调度完成了.eventProcessActor
          //在单元测试的时候是空的.
          // TODO: Cancel running tasks in the stage
          import env.actorSystem.dispatcher
          logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
            s"$failedStage (${failedStage.name}) due to fetch failure")
          env.actorSystem.scheduler.scheduleOnce(
            RESUBMIT_TIMEOUT, eventProcessActor, ResubmitFailedStages)
        }
        failedStages += failedStage
        failedStages += mapStage

        //标记在map stage中fetch failed的map
        if (mapId != -1) {
          mapStage.removeOutputLoc(mapId, bmAddress)
          mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
        }

        // TODO: mark the executor as failed only if there were lots of fetch failures on it
        if (bmAddress != null) {
          handleExecutorLost(bmAddress.executorId, Some(task.epoch))
        }

      case ExceptionFailure(className, description, stackTrace, metrics) =>
        //什么也不做,留给TaskScheduler决定这个如何处理用户的失败
      case TaskResultLost =>
        //什么也不做,留给TaskScheduler来处理和重新提交这些失败的task

      case other =>
        //无法识别失败,还是什么都不做,如果task反复失败,这个TaskScheduler将终止工作
    }
    submitWaitingStages()
  }

  /**
   *
   * 响应丢失的Executor.其在event loop的内部调用,因此假设他能修改调度程序的内部状态.使用executorLost()方法从外部
   * 提交loss event.
   *
   * 用以检测节点是否真的失联了,采取的方式就是选择一个时间点,放宽失败的条件,看看能不能重新触发节点探测功能.什么屁话...
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   *
   *
   */
  private[scheduler] def handleExecutorLost(execId: String, maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)
      // TODO: This will be really slow if we keep accumulating shuffle map stages
      for ((shuffleId, stage) <- shuffleToMapStage) {
        stage.removeOutputsOnExecutor(execId)
        val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray
        mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
      }
      if (shuffleToMapStage.isEmpty) {
        mapOutputTracker.incrementEpoch()
      }
      clearCacheLocs()
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
   *终止一个stage中的所有job.响应的task set被TaskScheuler取消.使用taskSetFailed()方法从外部注入事件.
   */
  private[scheduler] def abortStage(failedStage: Stage, reason: String) {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTime())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason")
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /**
   * Fails a job and all stages that are only used by that job, and cleans up relevant state.
   * 关掉一个所有stages都在运行的job,并且清理掉state
   */
  private def failJobAndIndependentStages(job: ActiveJob, failureReason: String) {
    val error = new SparkException(failureReason)
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    //取消所有的依赖, 运行stages
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError("Missing Stage for stage with id $stageId")
        } else {
          // 这是这个stage仅有的job,所以如果其正在运行将会失败
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try {
              //如果SchedulerBackend没有实现killTask就会导致cancelTasks失败
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              stage.latestInfo.stageFailed(failureReason)
              listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, JobFailed(error)))
    }
  }

  /**
   * 如果stage的ancestors是target就返回true
   */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    val visitedStages = new HashSet[Stage]
    //手动维护堆栈,防止由于递归访问导致栈溢出
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.jobId)
              if (!mapStage.isAvailable) {
                visitedStages += mapStage
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * 同步方法,可能从其他线程调用
   * 参数1_rdd:这个RDD的partitions将被咱们观看
   * 参数2_partitions:你观看partitions的位置
   * 返回:通过partitions选出的machines集合
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = synchronized {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /** getPreferredLocs的递归实现.
    * */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_],Int)])
    : Seq[TaskLocation] =
  {
    //如果这个partitions已经被访问了,就没必要再访问了,避免了重复访问
    if (!visited.add((rdd,partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    //如果这个partitions已经缓存了,就返回本地的缓存
    val cached = getCacheLocs(rdd)(partition)
    if (!cached.isEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    //如果这个RDD有一些位置设置等(作为输入的RDDs),得到这些
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (!rddPrefs.isEmpty) {
      return rddPrefs.map(host => TaskLocation(host))
    }
    //如果RDD是窄依赖,pick the first partition of the first narrow dep that has any placement preferences.
    //情理中,我们应该选择基于大小传输,但是要立即做,就顾不得那么多了
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    dagSchedulerActorSupervisor ! PoisonPill
    taskScheduler.stop()
  }
}

private[scheduler] class DAGSchedulerActorSupervisor(dagScheduler: DAGScheduler)
  extends Actor with Logging {

  override val supervisorStrategy =
    OneForOneStrategy() {
      case x: Exception =>
        logError("eventProcesserActor failed; shutting down SparkContext", x)
        try {
          dagScheduler.doCancelAllJobs()
        } catch {
          case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
        }
        dagScheduler.sc.stop()
        Stop
    }

  def receive = {
    case p: Props => sender ! context.actorOf(p)
    case _ => logWarning("received unknown message in DAGSchedulerActorSupervisor")
  }
}

private[scheduler] class DAGSchedulerEventProcessActor(dagScheduler: DAGScheduler)
  extends Actor with Logging {

  override def preStart() {
    //为taskScheduler设置DAGScheduler,当message到达的时候,确保eventProcessActor能验证
    dagScheduler.taskScheduler.setDAGScheduler(dagScheduler)
  }

  /**
   * DAG scheduler接受各种消息(重要)
   */
  def receive = {
    case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
        listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def postStop() {
    //取消任意job的postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  //等待即将到来的 fetch failure events
  //这是一个简单的方式来避免在non-fetchable map stage时,一个又一个错误的event到来,导致 重复提交task
  val RESUBMIT_TIMEOUT = 200.milliseconds

  //这个,唤醒完成队列,尽可能提交失败的stage
  val POLL_TIMEOUT = 10L
}
