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

package org.apache.spark.storage

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * 每个BlockManager的Storage信息
 *
 * 这个类假设BlockID和BlockStatus是不可变的,这个类不能改变信息来源.非线程安全.
 */
@DeveloperApi
class StorageStatus(val blockManagerId: BlockManagerId, val maxMem: Long) {

  /**
   * 在该block manager存储block 信息
   * //我们存储RDD blocks和non-RDD blocks去快富修复RDD的blocks
   * 这些集合应该只能被   add/update/removeBlcok 这些方法改变
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  /**
   *
   * 有关block存储的参数,内存,disk和非堆内存参数.,四个参数:
   *
   *   (memory size, disk size, off-heap size, storage level)
   *
   * 我们假设所有的block都属于相同的RDD,有相同的存储级别.
   * 这个fiedld和non-RDD是不相关的,non-RDD的存储仅有三个参数. (memory size, disk size, off-heap size,)
   *
   */
  private val _rddStorageInfo = new mutable.HashMap[Int, (Long, Long, Long, StorageLevel)]
  private var _nonRddStorageInfo: (Long, Long, Long) = (0L, 0L, 0L)

  /** 初始化block,创建一个storage状态.
    * */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   *返回该block manager存储的block
   *
   * 注意:这是有点代价昂贵的,因为他涉及了底层的map的复制,和他们的连接.有更常用替代操作符:contains,get,和size
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks

  /**
   * Return the RDD blocks stored in this block manager.
   *返回该block manager存储的 rdd block
   *
   *
   * 注意:这是有点代价昂贵的,因为他涉及了底层的map的复制,和他们的连接.
   * 常用的替代操作符:这个RDD的getting the memory,disk,和off-heap 内存大小
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }

    /** 返回早block manager中属于给定RDD的blocks*/
  /
  def rddBlocksById(rddId: Int): Map[BlockId, BlockStatus] = {
    _rddBlocks.get(rddId).getOrElse(Map.empty)
  }

  /** 增加给定块的存储状态,如果存在就覆写他*/
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /**  更新block的存储状态,如果不存在,就增加他*/
  private[spark] def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    addBlock(blockId, blockStatus)
  }

  /** 移除给定块的存储信息*/
  private[spark] def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    updateStorageInfo(blockId, BlockStatus.empty)
    blockId match {
      case RDDBlockId(rddId, _) =>
        // Actually remove the block, if it exists
        if (_rddBlocks.contains(rddId)) {
          val removed = _rddBlocks(rddId).remove(blockId)
          // If the given RDD has no more blocks left, remove the RDD
          if (_rddBlocks(rddId).isEmpty) {
            _rddBlocks.remove(rddId)
          }
          removed
        } else {
          None
        }
      case _ =>
        _nonRddBlocks.remove(blockId)
    }
  }

  /**
   * 判断给定的block是否存在block manager中,O(1)
   * 注意,这个比`this.blocks.contains`更快O(blocks)
   *
   */
  def containsBlock(blockId: BlockId): Boolean = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).exists(_.contains(blockId))
      case _ =>
        _nonRddBlocks.contains(blockId)
    }
  }

  /**
   * 返回block manager中给定的块 O(1)
   * 注意:这个比`thisblocks.get`更快
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).map(_.get(blockId)).flatten
      case _ =>
        _nonRddBlocks.get(blockId)
    }
  }

  /**
   * 返回block manager中存储的block的数量,O(Rdds)时间
   * 注意:这个比`this.blocks.size`更快
   */
  def numBlocks: Int = _nonRddBlocks.size + numRddBlocks

  /**
   * 返回该block manager中存储的RDD的blocks的数量
   * 注意:这个比`this.rddBlocks.size`更快
   */
  def numRddBlocks: Int = _rddBlocks.values.map(_.size).sum

  /**
   * 返回给定RDD中存储的blocks的数量
   *
   * 注意:这个比`this.rddBlocksById(rddId).size`更快,花费O(RDD中blocks)的时间
   */
  def numRddBlocksById(rddId: Int): Int = _rddBlocks.get(rddId).map(_.size).getOrElse(0)

  /** 返回该block manager剩余的内存
    * */
  def memRemaining: Long = maxMem - memUsed

  /** 返回该block manager使用的内存*/
  def memUsed: Long = _nonRddStorageInfo._1 + _rddBlocks.keys.toSeq.map(memUsedByRdd).sum

  /** 返回该block manager使用的disk大小. */
  def diskUsed: Long = _nonRddStorageInfo._2 + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** 返回该block manager使用的非堆内存大小 */
  def offHeapUsed: Long = _nonRddStorageInfo._3 + _rddBlocks.keys.toSeq.map(offHeapUsedByRdd).sum

  /**  返回这个block manager中给定的RDD所占用的内存,O(1)*/
  def memUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._1).getOrElse(0L)

  /**  返回这个block manager中给定的RDD所占用的disk大小,O(1)*/
  def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._2).getOrElse(0L)

  /**  返回这个block manager中给定的RDD所占用的非堆(off-Heap)内存,O(1)*/
  def offHeapUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._3).getOrElse(0L)

  /**  返回这个block manager 给定的RDD的存储级别*/
  def rddStorageLevel(rddId: Int): Option[StorageLevel] = _rddStorageInfo.get(rddId).map(_._4)

  /**
   * 更新相关的存储信息,taking into account any existing status for this block.
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val changeInTachyon = newBlockStatus.tachyonSize - oldBlockStatus.tachyonSize
    val level = newBlockStatus.storageLevel

    //从之前的信息计算新信息
    val (oldMem, oldDisk, oldTachyon) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case (mem, disk, tachyon, _) => (mem, disk, tachyon) }
          .getOrElse((0L, 0L, 0L))
      case _ =>
        _nonRddStorageInfo
    }
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)
    val newTachyon = math.max(oldTachyon + changeInTachyon, 0L)

    //设置正确的信息
    blockId match {
      case RDDBlockId(rddId, _) =>
        //如果这个RDD不是持久化的,删除之
        if (newMem + newDisk + newTachyon == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = (newMem, newDisk, newTachyon, level)
        }
      case _ =>
        _nonRddStorageInfo = (newMem, newDisk, newTachyon)
    }
  }

}

/** 和存储对象有关的帮助方法*/
private[spark] object StorageUtils {

  /**
   * 随着存储状态的变化,不断更新给定RDDInfo的信息
   * 这个方法覆写了RDDInfo的原始值
   */
  def updateRddInfo(rddInfos: Seq[RDDInfo], statuses: Seq[StorageStatus]): Unit = {
    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id
      //假设所有的blocks属于通过一个RDD,同一个存储级别
      val storageLevel = statuses
        .flatMap(_.rddStorageLevel(rddId)).headOption.getOrElse(StorageLevel.NONE)
      val numCachedPartitions = statuses.map(_.numRddBlocksById(rddId)).sum
      val memSize = statuses.map(_.memUsedByRdd(rddId)).sum
      val diskSize = statuses.map(_.diskUsedByRdd(rddId)).sum
      val tachyonSize = statuses.map(_.offHeapUsedByRdd(rddId)).sum

      rddInfo.storageLevel = storageLevel
      rddInfo.numCachedPartitions = numCachedPartitions
      rddInfo.memSize = memSize
      rddInfo.diskSize = diskSize
      rddInfo.tachyonSize = tachyonSize
    }
  }

  /** 返回block id 和其所属于的rdd 的 映射(map)*/
  def getRddBlockLocations(rddId: Int, statuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    statuses.foreach { status =>
      status.rddBlocksById(rddId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }

}
