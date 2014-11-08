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

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

/**
 * :: Experimental ::
 * 支持取消的action的结果集.这是scala支持取消的功能的的接口扩展
 *
 */
@Experimental
trait FutureAction[T] extends Future[T] {
  //注意:我们重新定义方法接口,在此重新指定一个document(重新描述action)
  /**
   * 取消action的执行
   */
  def cancel()

  /**
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @return this FutureAction
   *
   *阻塞,直到action完成.
   *
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * 等待并且返回action结果的类型
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration(持续).Inf
   *               for unbounded(无边界的) waiting, or a finite(有限) positive duration
   * @throws Exception exception during action execution
   * @return action在指定时间内完成得到的结果集
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
   * When this action is completed, either through an exception, or a value, applies the provided
   * function.
   *
   * 当这个action完成之后,通过提供的方法,要不得到一个结果值,要不得到一个exception
   *
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext)

  /**
   * 判断action是否已经完成
   */
  override def isCompleted: Boolean

  /**
   *
   * 将要计算出来的值.
   * 如果这个值没有计算出来,将返回Node.如果这个值计算出来了,如果包含验证的结果集,就返回Some(Success(t)),如果包含
   * 一个异常,就返回Some(Failure(error))
   */
  override def value: Option[Try[T]]

  /**
   * 阻塞并且返回这个job的结果集.
   */
  @throws(classOf[Exception])
  def get(): T = Await.result(this, Duration.Inf)
}


/**
 * :: Experimental ::
 * 一个[[FutureAction]]触发一个job,并且保留action的结果集.例如有count,collect,reduce
 *
 */
@Experimental
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  override def cancel() {
    jobWaiter.cancel()
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    if (!atMost.isFinite()) {
      awaitResult()
    } else jobWaiter.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobWaiter.wait(finishTime - time)
        }
      }
    }
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  private def awaitResult(): Try[T] = {
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc)
      case JobFailed(e: Exception) => scala.util.Failure(e)
    }
  }
}


/**
 * :: Experimental ::
 *一个[[FutureAction]]能够触发多个Spark的jobs.例如take,takeSample.
 * 通过设置 cancelled flag为true,取消works并且如果阻塞了这个job就中断线程的action
 */
@Experimental
class ComplexFutureAction[T] extends FutureAction[T] {

  //执行action的线程,当action运行的时候,他将被设置
  @volatile private var thread: Thread = _

  //表明这个值是否会被取消.被用来在action运行之前.(并且我们此时没有线程中断)
  @volatile private var _cancelled: Boolean = false

  // A promise used to signal the future.
  private val p = promise[T]()

  override def cancel(): Unit = this.synchronized {
    _cancelled = true
    if (thread != null) {
      thread.interrupt()
    }
  }

  /**
   * Executes some action enclosed in the closure. To properly enable cancellation, the closure
   * should use runJob implementation in this promise. See takeAsync for example.
   *
   * 执行一些enclosed的Action.
   * 对于可能能够取消的job,应该使用runJob实现类根据能否取消的设置结束......参见:takeAsync for example.
   *
   */
  def run(func: => T)(implicit executor: ExecutionContext): this.type = {
    scala.concurrent.future {
      thread = Thread.currentThread
      try {
        p.success(func)
      } catch {
        case e: Exception => p.failure(e)
      } finally {
        thread = null
      }
    }
    this
  }

  /**
   * 运行Sparkd的job.封装了SparkContext提供的能够取消的相同方法.
   */
  def runJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R) {
    // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
    // command need to be in an atomic block.
    //如果这个action已经被取消了,提交这个job.检查submitJob命令需要的atomic block.
    val job = this.synchronized {
      if (!cancelled) {
        rdd.context.submitJob(rdd, processPartition, partitions, resultHandler, resultFunc)
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }
    //等待这个job完成.如果这个action取消了(有一个中断),停止执行这个job.这不是同步的,因为Await.ready
    //最终在FutureJob.jobWaiter上等待监控

    try {
      Await.ready(job, Duration.Inf)
    } catch {
      case e: InterruptedException =>
        job.cancel()
        throw new SparkException("Action has been cancelled")
    }
  }

  /**
   * 根据能否取消的设置
   */
  def cancelled: Boolean = _cancelled

  @throws(classOf[InterruptedException])
  @throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value
}
