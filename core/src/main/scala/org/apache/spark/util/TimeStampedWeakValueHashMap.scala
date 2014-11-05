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

package org.apache.spark.util

import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.Logging

/**
 *对TimeStampedHashMap的封装,确保values是弱参照和时间戳
 *如果该值是废弃的集合和弱引用是null,get()方法将返回一个不存在的value,这个类在map的periodically中是删除的,
 * (每隔N次插入),作为他们的值不再是强引用.进一步来说,键-值对的时间戳是不是一个特定的阈值，
 * 可以使用clearoldvalues方法删除旧的。
 *TimeStampedWeakValueHashMap 是使用scala.collection.mutable.Map接口实现的,这使得他能被scala的HashMaps替换
 *本质上,他使用了一个java的ConcurrentHashMap,所以所有的在HashMap上的操作是线程安全的.
 *
 * 参数1_updateTimeStampOnGet:当访问的时候更新timestamp对
 */
private[spark] class TimeStampedWeakValueHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends mutable.Map[A, B]() with Logging {

  import TimeStampedWeakValueHashMap._

  private val internalMap = new TimeStampedHashMap[A, WeakReference[B]](updateTimeStampOnGet)
  private val insertCount = new AtomicInteger(0)

  /**
    * 当entries的值是强引用额时候返回一个包含entries的map
    * */
  private def nonNullReferenceMap = internalMap.filter { case (_, ref) => ref.get != null }

  def get(key: A): Option[B] = internalMap.get(key)

  def iterator: Iterator[(A, B)] = nonNullReferenceMap.iterator

  override def + [B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B1]
    val oldMap = nonNullReferenceMap.asInstanceOf[mutable.Map[A, WeakReference[B1]]]
    newMap.internalMap.putAll(oldMap.toMap)
    newMap.internalMap += kv
    newMap
  }

  override def - (key: A): mutable.Map[A, B] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B]
    newMap.internalMap.putAll(nonNullReferenceMap.toMap)
    newMap.internalMap -= key
    newMap
  }

  override def += (kv: (A, B)): this.type = {
    internalMap += kv
    if (insertCount.incrementAndGet() % CLEAR_NULL_VALUES_INTERVAL == 0) {
      clearNullValues()
    }
    this
  }

  override def -= (key: A): this.type = {
    internalMap -= key
    this
  }

  override def update(key: A, value: B) = this += ((key, value))

  override def apply(key: A): B = internalMap.apply(key)

  override def filter(p: ((A, B)) => Boolean): mutable.Map[A, B] = nonNullReferenceMap.filter(p)

  override def empty: mutable.Map[A, B] = new TimeStampedWeakValueHashMap[A, B]()

  override def size: Int = internalMap.size

  override def foreach[U](f: ((A, B)) => U) = nonNullReferenceMap.foreach(f)

  def putIfAbsent(key: A, value: B): Option[B] = internalMap.putIfAbsent(key, value)

  def toMap: Map[A, B] = iterator.toMap

  /**
    * 删除时间戳比threshTime更早的k-v对
    * */
  def clearOldValues(threshTime: Long) = internalMap.clearOldValues(threshTime)

  /**
    * 删除有强引用值的entries
    * */
  def clearNullValues() {
    val it = internalMap.getEntrySet.iterator
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getValue.value.get == null) {
        logDebug("Removing key " + entry.getKey + " because it is no longer strongly reachable.")
        it.remove()
      }
    }
  }

  // For testing

  def getTimestamp(key: A): Option[Long] = {
    internalMap.getTimeStampedValue(key).map(_.timestamp)
  }

  def getReference(key: A): Option[WeakReference[B]] = {
    internalMap.getTimeStampedValue(key).map(_.value)
  }
}

/**
 *弱引用的转换辅助方法
 */
private object TimeStampedWeakValueHashMap {

  // Number of inserts after which entries with null references are removed
  val CLEAR_NULL_VALUES_INTERVAL = 100

  /* Implicit conversion methods to WeakReferences. */

  implicit def toWeakReference[V](v: V): WeakReference[V] = new WeakReference[V](v)

  implicit def toWeakReferenceTuple[K, V](kv: (K, V)): (K, WeakReference[V]) = {
    kv match { case (k, v) => (k, toWeakReference(v)) }
  }

  implicit def toWeakReferenceFunction[K, V, R](p: ((K, V)) => R): ((K, WeakReference[V])) => R = {
    (kv: (K, WeakReference[V])) => p(kv)
  }

  /* Implicit conversion methods from WeakReferences. */

  implicit def fromWeakReference[V](ref: WeakReference[V]): V = ref.get

  implicit def fromWeakReferenceOption[V](v: Option[WeakReference[V]]): Option[V] = {
    v match {
      case Some(ref) => Option(fromWeakReference(ref))
      case None => None
    }
  }

  implicit def fromWeakReferenceTuple[K, V](kv: (K, WeakReference[V])): (K, V) = {
    kv match { case (k, v) => (k, fromWeakReference(v)) }
  }

  implicit def fromWeakReferenceIterator[K, V](
      it: Iterator[(K, WeakReference[V])]): Iterator[(K, V)] = {
    it.map(fromWeakReferenceTuple)
  }

  implicit def fromWeakReferenceMap[K, V](
      map: mutable.Map[K, WeakReference[V]]) : mutable.Map[K, V] = {
    mutable.Map(map.mapValues(fromWeakReference).toSeq: _*)
  }
}
