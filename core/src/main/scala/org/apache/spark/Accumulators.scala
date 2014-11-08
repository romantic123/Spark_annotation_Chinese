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

import java.io.{ObjectInputStream, Serializable}

import scala.collection.generic.Growable
import scala.collection.mutable.Map
import scala.reflect.ClassTag

import org.apache.spark.serializer.JavaSerializer

/**
 *
 * 一个能够被累加的数据类型.他是一个只有add的操作.结果类型"R"可能会和增加的element的类型'T'不一样.
 *
 * 你必须定义怎样添加一个data,怎样合并.对于一些数据类型,比如counter,可能有相同的操作.在这点上,你可以用[[org.apache.spark.Accumulator]],
 * 他们不总是相同的,你可以想象一下,你正在accumulating的一个集合.你讲添加items到这个集合,并且你将两个set联合在一起.
 *
 * 参数1_initialValue:初始化的accumulator值
 * 参数2_param:定义添加元素类型'R'和'T'的帮助对象
 * 参数3_name:可读的名字,用于在Spark的Web UI中显示
 * 参数4_R:结果类型
 * 参数5_T:能被添加的局部数据
 *
 *
 */
class Accumulable[R, T] (
    @transient initialValue: R,
    param: AccumulableParam[R, T],
    val name: Option[String])
  extends Serializable {

  def this(@transient initialValue: R, param: AccumulableParam[R, T]) =
    this(initialValue, param, None)

  val id: Long = Accumulators.newId

  @transient private var value_ = initialValue //Master的当前值
  val zero = param.zero(initialValue)  //zero值传递给Workers
  private var deserialized = false

  Accumulators.register(this, true)

  /**
   * 增加更多的数据到   accumulator/accumulable
   * 参数1_term:增加的数据
   */
  def += (term: T) { value_ = param.addAccumulator(value_, term) }

  /**
   * 增加更多的数据到   accumulator/accumulable
   * 参数1_term:增加的数据
   */
  def add(term: T) { value_ = param.addAccumulator(value_, term) }

  /**
   * 合并两个accumulable到一起
   *通常用户不会想到用这个,只会想到使用'+='
   *
   * 参数1_term:另一个将要和这个合并的R
   */
  def ++= (term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * 合并两个accumulable到一起
   *通常用户不会想到用这个,只会想到使用'+='
   *
   * 参数1_term:另一个将要和这个合并的R
   */
  def merge(term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * 访问累加器的当前值,只能够是master才允许
   */
  def value: R = {
    if (!deserialized) {
      value_
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * 从一个task中获取accumulator的当前值
   *
   * 这个不是全局的累加器的值.得到全局只在完成数据.在数据集上的操作完成之后,获取全局值.叫做'call'
   *
   * 典型使用这个方法是直接改变本地值,例如:增加一个元素到集合中
   *
   *
   */
  def localValue = value_

  /**
   * 设置累加器的值,只允许master设置
   */
  def value_= (newValue: R) {
    if (!deserialized) {
      value_ = newValue
    } else {
      throw new UnsupportedOperationException("Can't assign accumulator value in task")
    }
  }

  /**
   * 设置累加器的值,近允许master
   */
  def setValue(newValue: R) {
    this.value = newValue
  }

  //当反序列化睇相的时候,调用java
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = if (value_ == null) "null" else value_.toString
}

/**
 *
 *定义怎么累加值的帮助对象.当你对指定类型创建[[Accumulable]],一个隐式的AccumulableParam是很有用的
 *
 * 参数1_R:结果集类型
 * 参数2_T:能够被增加的data
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * 增加一个额外的data到累加器.允许修改,效率高(避免分配对象),可修改.返回'r'
   *
   * 参数1_r:累加器的当前值
   * 参数2_t:累加器增加的data
   * 返回:累加器的新值
   *
   */
  def addAccumulator(r: R, t: T): R

  /**
   *
   * 合并两个累加的值.允许修改,并且高效(避免分配对象)的返回第一个值.
   * 参数r1:累加值的集合
   * 参数r2:另一个累加值的集合
   * 返回:合并后的值
   *
   */
  def addInPlace(r1: R, r2: R): R

  /**
   *
   *对一个累加器给定的初始值返回0值.例如:如果R是一个N维向量,这将返回一个N维0值向量.
   *
   */
  def zero(initialValue: R): R
}

private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // 取而代之,我们就将其序列化,并且负载
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}

/**
 * 在[[Accumulable]]]中相同的类型的元素将被合并.换言之,变量仅仅是通过关联操作"added",所以能有效的支持并行.
 * 他们能被用来实现counters或者sums.Spark生来就支持numeric类型的累加器,并且程序员能够增加对新类型的支持.

 *
 * 从一个初始值'v'创建一个累加器,通过调用[[SparkContext#accumulator]].
 * 在集群中运行的tasks能使用[[Accumulable#+=]]增加data.
 * 然而,他们不能读取他的值.仅有driver能够读取accumulator的值,使用他的value方法
 *
 *
 * 下面的解释了一个累加器用于添加一个数组元素
 *
 * {{{
 * scala> val accum = sc.accumulator(0)
 * accum: spark.Accumulator[Int] = 0
 *
 * scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
 * ...
 * 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
 *
 * scala> accum.value
 * res2: Int = 10
 * }}}
 *
 * 参数1_initialValue:累加器的初始值
 * 参数2_param:定义怎样增加元素类型T的帮助对象
 * 参数3_T:结果类型
 */
class Accumulator[T](@transient initialValue: T, param: AccumulatorParam[T], name: Option[String])
    extends Accumulable[T,T](initialValue, param, name) {
  def this(initialValue: T, param: AccumulatorParam[T]) = this(initialValue, param, None)
}

/**
 *
 * 一个更简单的版本[[org.apache.spark.AccumulableParam]] ,你只能添加相同类型的数据元素,作为累加值.当你创建一个特定
 * 类型的累加器的时候,需要隐式设置AccumulatorParam对象
 *
 * 参数_1:累加器值的类型
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators {
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulable[_, _]]()
  val localAccums = Map[Thread, Map[Long, Accumulable[_, _]]]()
  var lastId: Long = 0

  def newId: Long = synchronized {
    lastId += 1
    lastId
  }

  def register(a: Accumulable[_, _], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val accums = localAccums.getOrElseUpdate(Thread.currentThread, Map())
      accums(a.id) = a
    }
  }

  //清除当前线程的累加器
  def clear() {
    synchronized {
      localAccums.remove(Thread.currentThread)
    }
  }

  //获取当前线程(通过ID)的本地累加值
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, Map())) {
      ret(id) = accum.localValue
    }
    return ret
  }

  //根据给定的IDs,把值增加到原始的累加器中
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[Accumulable[Any, Any]] ++= value
      }
    }
  }

  def stringifyPartialValue(partialValue: Any) = "%s".format(partialValue)
  def stringifyValue(value: Any) = "%s".format(value)
}
