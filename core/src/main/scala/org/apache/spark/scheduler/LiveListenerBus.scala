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

import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 *
 *异步传输Spark监听事件给注册的SparkListeners
 *直到start()方法被调用,否则所有提交的事件都是缓冲.只有这个方法start()了之后,实际的消息才会发送给各个监听器.当
 * 这个linsteners bus接收到sparkListererShutdown关闭的时间时,才会停止
 *
 */
private[spark] class LiveListenerBus extends SparkListenerBus with Logging {

/**如果他的添加速度比读取速度快的话.当超过SparkListererEvent队列长度的时候,我们将得到一个明确的错误(而不是OOM异常)
 * */
   private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)
  private var queueFullErrorMessageLogged = false
  private var started = false

  /**一个计数器,代表在队列中生产和消费的事件数量*/
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread("SparkListenerBus") {
    setDaemon(true)
    override def run(): Unit = Utils.logUncaughtExceptions {
      while (true) {
        eventLock.acquire()
        //自动清除和处理这个事件
        LiveListenerBus.this.synchronized {
          val event = eventQueue.poll
          if (event == SparkListenerShutdown) {
           //推出while循环并且关闭守护进程
            return
          }
          Option(event).foreach(postToAll)
        }
      }
    }
  }

  /**
   * 开始向时间监听器发送事件,当listener bus开始启动的时候,会将所有缓存的事件都发送出去,之后listener bus仍然运行,
   * 监听一些异步事件.该方法只能被调用一次.
   */
  def start() {
    if (started) {
      throw new IllegalStateException("Listener bus already started!")
    }
    listenerThread.start()
    started = true
  }

  def post(event: SparkListenerEvent) {
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      logQueueFullErrorMessage()
    }
  }

  /**
   * 该方法仅仅仅供测试.一直等到事件队列为空,或者到达指定的时间.如果队列是空的或者在队列未空之间设置的时间到了,都返回true
   */
  def waitUntilEmpty(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
    true
  }

  /**
   * 仅供测试.判断监听器的守护进程是否还处于激活状态.
   */
  def listenerThreadIsAlive: Boolean = synchronized { listenerThread.isAlive }

  /**
   * 判断队列是否是空的.
   *使用同步方法来保证所有属于该队列的事件都被listeners处理了,如果是,就返回true
   */
  def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty }

  /**
   *记录一个错误信息,并指出事件队列已经满了.只做一次.
   */
  private def logQueueFullErrorMessage(): Unit = {
    if (!queueFullErrorMessageLogged) {
      if (listenerThread.isAlive) {
        logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
          "This likely means one of the SparkListeners is too slow and cannot keep up with" +
          "the rate at which tasks are being started by the scheduler.")
      } else {
        logError("SparkListenerBus thread is dead! This means SparkListenerEvents have not" +
          "been (and will no longer be) propagated to listeners for some time.")
      }
      queueFullErrorMessageLogged = true
    }
  }

  def stop() {
    if (!started) {
      throw new IllegalStateException("Attempted to stop a listener bus that has not yet started!")
    }
    post(SparkListenerShutdown)
    listenerThread.join()
  }
}
