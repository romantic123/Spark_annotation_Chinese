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

package org.apache.spark.broadcast

import java.io.Serializable

import org.apache.spark.SparkException

import scala.reflect.ClassTag

/**
 *
 * 一个广播变量.广播变量允许程序员定义一个只读的变量,缓存到每个机器中,而不是传输他的副本.很多地方可以使用他们:
 * 例如,给每个节点一个大量数据集的副本,用于有效的管理.Spark也尝试有效的分发广播变量,减少通信花费.
 *
 * 广播变量被变量v,通过调用[[org.apache.spark.SparkContext#broadcast]]后创建
 *
 * 这个广播变量封装v,可以通过调用'value'方法,来访问他的值,如同下面:
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: spark.Broadcast[Array[Int]] = spark.Broadcast(b5c40191-a864-4c7d-b9bf-d87e1a4e787c)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 *在广播变量创建以后,他应该被用来代替值`v`运行在集群中,这样,`v`就不要再发送给集群中的各个节点了
 * 对象`v`应该不能修改,以便所有节点的广播变量都有相同的值
 *
 * 参数1_id:广播变量的唯一标识值
 * 参数2_T:广播变量的data的类型
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable {

  /**
   *
   * flag代表广播变量是否是有效的
   *
   */
  @volatile private var _isValid = true

  /**得到广播变量的值*/
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   *
   * 异步删除Executor上的广播副本.
   * 这个广播在调用之后,将需要重新发给每个Executor
   *
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   *
   * 删除Executor上的广播变量副本.如果这个广播变量在调用之后被使用,他将重新发给每个Executor.
   *
   * 参数1_blocking:block的unpersisting过程是否已经完成.
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }

  /**
   * 销毁和广播变量所有有关的data和metadata.
   * 使用该方法要小心,因为一旦广播变量销毁了,就不能再用了
   *
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    doDestroy(blocking)
  }

  /**
   *
   * 这个广播变量是否实际可用.一旦持久化状态从driver中删除,就返回false
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * 得到广播变量的值.广播的具体实现类必须定义得到值的方法.
   *
   */
  protected def getValue(): T

  /**
   * 在executors实际执行的unpersist的广播变量.广播的具体实现类必须定义他们自己的unpersist他们数据的方法
   *
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * 销毁所有和广播变量有关的data和metadata的值.广播的具体实现类也必须定义销毁他们状态的方法
   *
   */
  protected def doDestroy(blocking: Boolean)

  /** 检查广播变量是否被验证,如果没有,抛出异常
    * */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException("Attempted to use %s after it has been destroyed!".format(toString))
    }
  }

  override def toString = "Broadcast(" + id + ")"
}
