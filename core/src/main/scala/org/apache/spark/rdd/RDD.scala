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

package org.apache.spark.rdd

import java.util.Random

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, SamplingUtils}

/**
 *
 * 弹性数据集(RDD),是Spark的基本的抽象.代表一个不可变,可并行操作的元素集合.这个类包括了对RDD的基本操作.例如,map,
 * filter,persist.除此之外:
 *[[org.apache.spark.rdd.PairRDDFunctions]]有一个很有用的操作符,对k-v形式的RDD操作,
 * 比如:`groupByKey` and `join`;
 *
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]]:包含了对Double型的RDD的操作
 *
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]]:包含了能够存为SequenceFiles的RDD的操作
 *
 *这些操作符号对RDD的类型自动匹配操作(例如:RDD[(Int,Int)],当你"import org.apache.spark.SparkContext._"会自动的
 * 隐式转换)
 * RDD:
 * 一系列的分片(partitions)
 * 切片上的计算函数
 * RDD依赖于其他RDD
 * 对k-v rdd的partitions
 * 对每个数据分片的预定义地址列表(例如HDFS上的数据块的地址)
 *
 * Spark的所有调度都是基于RDD完成的,每个RDD都会对定义对自己的操作.确实,用户可以通过覆写方法实现custom RDDs(流:从存储系统读取数据)
 * 参照[[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]]了解更多的rdd细节
 *
 *
 */
abstract class RDD[T: ClassTag](
    @transient private var sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf
  // =======================================================================
  //应该被子RDD实现的方法
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * 通过子类来计算一个给定的分区,这里的计算函数是针对每个partition的操作
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   *
   * 通过子类返回该RDD的partitions的集合.这个方法只能被调用一次,所以其在实现耗时计算时是安全的.
   *
   */
  protected def getPartitions: Array[Partition]

  /**
   * 通过子类返回该RDD依赖的父RDD.这个方法只能被调用一次,所以其在实现耗时计算时是安全的.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**[可选]子类覆写指定的地址列表
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /**[可选]子类覆写,用于指定他们是如何分区(partitioned)的*/
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  //对所有RDD的方法和fields都很有用
  // =======================================================================

  /** 创建此RDD的SparkContext */
  def sparkContext: SparkContext = sc

  /** 这个RDD的唯一ID()
    * */
  val id: Int = sc.newRddId()

  /** 这个RDD的名字 */
  @transient var name: String = null

  /** 分配给这个RDD一个名字*/
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * 设置RDD的存储级别去持久化他的值,在第一次计算之后.仅仅用来设置一个新的存储级别,如果这个RDD还没有设置存储级别.
   */
  def persist(newLevel: StorageLevel): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    sc.persistRDD(this)
    //给这个RDD注册一个ContextCleaner,用来自动GC清理
    sc.cleaner.foreach(_.registerRDDForCleanup(this))
    storageLevel = newLevel
    this
  }

  /**持久化这个RDD的默认存储级别是(`MEMORY_ONLY`) */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /** 持久化这个RDD的默认存储级别是(`MEMORY_ONLY`) */
  def cache(): this.type = persist()

  /**
   *对这个RDD解除持久化,从内存和磁盘删除所有的blocks
   * 参数1_blocking:是否所有的blocks都要删除
   * 返回值:这个RDD
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    this
  }

  /**得到这个RDD当前的存储级别,如果没有设置就设为NONE  */
  def getStorageLevel = storageLevel

  //我们的依赖和分区将通过下面子类方法的调用得到,并且当我们checkpointed的时候猛将被覆写
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null

  /** 如果我们checkpointed,将返回一个具有checkpointRDD的Option
    * */
  private def checkpointRDD: Option[RDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * 得到这个RDD的依赖集合,考虑RDD是否checkpointed
   */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
   * 得到这个RDD的partitions,考虑是否checkpointed
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
      }
      partitions_
    }
  }

  /**
   * 得到一个partitions的位置设置,考虑是否checkpointed
   *
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * 这个RDD的内部方法,如果缓存可用,将从缓存读取,或者计算.
   * 这个方法不被用户直接调用,但是可用于开发RDD子类的实现
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * 通过一些列的窄依赖,返回和给定RDD依赖的ancestors.使用深度优先遍历RDD的依赖,但是返回的RDD是无序的
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]) {
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      val narrowParents = narrowDependencies.map(_.rdd)
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * 如果RDD已经checkpointing了,那么就从checkpoint(检查点),计算或者读取这个RDD partitions
   *
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointed) firstParent[T].iterator(split, context) else compute(split, context)
  }

  // Transformations (return a new RDD)

  /**
   *对该RDD中的所有元素都应用一个函数,并且返回一个新的RDD
   */
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))

  /**
   *  类似于map,但是flatmap把所有item都压平为一个
   *  也是对RDD中的所有元素都应用这个方法,但是会把结果压平
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))

  /**
   * 非常常用的功能,内部使用返回布尔值的方法,对RDD中的每个data使用该方法,返回result RDD
   */
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  /**
   * 返回一个新的RDD,这个RDD包含的是唯一值,把重复的都会去掉
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

  /**
   * 返回一个新的RDD,这个RDD包含的是唯一值,把重复的都会去掉
   */
  def distinct(): RDD[T] = distinct(partitions.size)

  /**
   * 用于碎片整合合并,增加减少partitions,提高并行度,(Spark的性能优化之一)
   * 返回一个具有numPartitions(你指定)个数的partitions的RDD
   *
   * 可以增加或者减少RDD的并行程度.如果filter导致结果RDD很稀疏,可能会有很多空的task(执行时间<20ms的task).
   * 你可以用这个shuffle方法,重新划分数据
   *
   * 如果你减少RDD的partitions的数量,你可以用coalesce,能够避免执行shuffle
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * 返回一个具有numPartitions(你指定)个数的partitions的RDD(Spark的性能优化之一)
   *
   * 这个结果返回的是窄依赖.例如:1000到100个partitions,将不会产生shuffle操作,取而代之,是
   * 100个新的partitions都将合并10个当前的partitions
   *
   * 然而,如果你正在做一个coalesce操作,比如numpartions=1.这可能导致的大量的计算都几种在很少一部分节点
   * (例如:都发生在一个节点),为了避免这种情况,你可以设置shuffle=true,这将导致一个shuffle的步骤,但是这意味着
   *当前的划分,将会并行执行(无论当前的partitions是什么)
   *
   * 注意:shuffle=true,你能合并大量的partitions.如果你有小数量的partitions,这个方法是非常有用的
   * 比如有100个partitions,可能有一部分的partitions是非常大的.调用这个方法coalesce(1000,shuffle=true)
   * 将使用hash让100个partitions均匀分布到1000个partitions中
   *
   *
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
      : RDD[T] = {
    if (shuffle) {
      /** Distributes elements evenly across output partitions, starting from a random partition. */
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions)
        items.map { t =>
          //计算key的hashcode,然后作为自己的key,这个HashPartitioner将对总共的partitions个数进行mod计算
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]

      //包括一个shuffle操作,以便能够分布式的upstream任务
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
        new HashPartitioner(numPartitions)),
        numPartitions).values
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

  /**
   * 在RDD中随机选择一个items
   */
  def sample(withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }

  /**
   * 随机选择一个RDD,并且赋予权重
   *
   * 参数1_weight:切片的权重,和为1
   * 参数2_seed:随机种子
   * 返回:切片rdd组成的数组
   *
   */
  def randomSplit(weights: Array[Double], seed: Long = Utils.random.nextLong): Array[RDD[T]] = {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](x(0), x(1)), true, seed)
    }.toArray
  }

  /**
   * 返回一个固定大小样本集的RDD数组
   *
   * 参数1_withReplacement:
   * 参数2_num:返回样本的大小
   * 参数3_seed:随机生成的seed数
   * 返回:指定数组的样本大小
   *
   */
  def takeSample(withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = {
    val numStDev =  10.0

    if (num < 0) {
      throw new IllegalArgumentException("Negative number of elements requested")
    } else if (num == 0) {
      return new Array[T](0)
    }

    val initialCount = this.count()
    if (initialCount == 0) {
      return new Array[T](0)
    }

    val maxSampleSize = Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt
    if (num > maxSampleSize) {
      throw new IllegalArgumentException("Cannot support a sample size > Int.MaxValue - " +
        s"$numStDev * math.sqrt(Int.MaxValue)")
    }

    val rand = new Random(seed)
    if (!withReplacement && num >= initialCount) {
      return Utils.randomizeInPlace(this.collect(), rand)
    }

    val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
      withReplacement)

    var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

    //如果第一个sample不足够的大,尝试着再取
    //不该经常发生,因为我门使用了一个大的乘数来初始化
    var numIters = 0
    while (samples.length < num) {
      logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
      samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
      numIters += 1
    }

    Utils.randomizeInPlace(samples, rand).take(num)
  }

  /**
   *
   * A 联合B ,类似数据库中的union.重复元素会出现多次(使用distinct来去重)
   */
  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  /**
   * 返回两个RDD的union值.相同的值将出现多次(使用distinct()来去重)
   */
  def ++(other: RDD[T]): RDD[T] = this.union(other)

  /**
   * 返回使用给定方法的RDD排序
   */
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.size)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =
    this.keyBy[K](f)
        .sortByKey(ascending, numPartitions)
        .values

  /**
   *
   * 返回两个RDD的交叉.数出的结果将不会包含任何重复的元素,尽管输入的RDD是重复的.
   * 注意:这个方法会执行shuffle
   */
  def intersection(other: RDD[T]): RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * 返回两个RDD的交叉.数出的结果将不会包含任何重复的元素,尽管输入的RDD是重复的.
   * 注意:这个方法会执行shuffle
   *
   * 参数1_partitioner:用于对结果集分片的partitioner
   *
   */
  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null)
      : RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * 返回两个RDD的交叉.数出的结果将不会包含任何重复的元素,尽管输入的RDD是重复的.
   * 注意:这个方法会执行shuffle
   *
   * 参数1_partitioner:对结果RDD进行分片的数量.
   *
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), new HashPartitioner(numPartitions))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * 把所有partition的元素都组合到一个RDD中来 .
   */
  def glom(): RDD[Array[T]] = new GlommedRDD(this)

  /**
   * 生成笛卡尔积
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

  /**
   *
   * 按照k对RDD进行分组.其中每组都包含一个k,和k相对应的序列
   *
   * 注意:这个操作符,非常费时.如果你是为了执行一个聚合分组(例如,求和,求平局),使用[[PairRDDFunctions.aggregateByKey]]
   * 或者[[PairRDDFunctions.reduceByKey]]会有更好的性能.
   *
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    groupBy[K](f, defaultPartitioner(this))

  /**
   * 按照k对RDD进行分组.其中每组都包含一个k,和k相对应的序列
   *
   * 注意:这个操作符,非常费时.如果你是为了执行一个聚合分组(例如,求和,求平局),使用[[PairRDDFunctions.aggregateByKey]]
   * 或者[[PairRDDFunctions.reduceByKey]]会有更好的性能.
   */
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    groupBy(f, new HashPartitioner(numPartitions))

  /**
   * 按照k对RDD进行分组.其中每组都包含一个k,和k相对应的序列
   *
   * 注意:这个操作符,非常费时.如果你是为了执行一个聚合分组(例如,求和,求平局),使用[[PairRDDFunctions.aggregateByKey]]
   * 或者[[PairRDDFunctions.reduceByKey]]会有更好的性能.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
      : RDD[(K, Iterable[T])] = {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
   * 就是在pipe中能够使用shell中的命令,并将结果返回未RDD的格式
   */
  def pipe(command: String): RDD[String] = new PipedRDD(this, command)

  /**
   *就是在pipe中能够使用shell中的命令,并将结果返回未RDD的格式
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] =
    new PipedRDD(this, command, env)

  /**
   * 就是在pipe中能够使用shell中的命令,并将结果返回未RDD的格式
   * 可以通过提供的两个方法,自定义打印行为
   *
   * 参数1_command:运行命令的字符串
   * 参数2_env:设置的环境变量
   * 参数3_printPipeContext:打印pipe context data
   * 参数4_printRDDElement:使用这个方法定义怎么pipe元素.这个方法将被每个RDD元素的第一个参数调用,第二个参数
   *
   * @param printPipeContext Before piping elements, this function is called as an oppotunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
   *                          for (e <- record._2){f(e)}
   * 参数5_separateWorkinghDir:对每个task使用单独的目录
   * return:返回结果RDD
   */
  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false): RDD[String] = {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir)
  }

  /**
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   *
   * 每个分区内进行map，物理分治的map，flatmap（map+flat集合合并压平）
   * 类似于hadoop中的reduce,类似于上面的map,独立的在RDD的每一个分块上运行---不太懂
   * `preservesPartitioning`:指出输入方法是否保留partitioner,默认是false,除非RDD的输入方法不能修改k值
   * */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   *在RDD的每个Partitions上都执行这个方法,并且跟踪Partitions的索引
   *
   *   * 每个分区内进行map，物理分治的map，flatmap（map+flat集合合并压平）
   * 类似于hadoop中的reduce,类似于上面的map,独立的在RDD的每一个分块上运行--
   * `preservesPartitioning`:指出输入方法是否保留partitioner,默认是false,除非RDD的输入方法不能修改k值

   */
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(index, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * :: DeveloperApi ::
   * 在RDD的每个Partitions上都执行这个方法,是mapPartitions的延伸的,可以使用taskContext关闭
   *
   * 每个分区内进行map，物理分治的map，flatmap（map+flat集合合并压平）
   * 类似于hadoop中的reduce,类似于上面的map,独立的在RDD的每一个分块上运行
   * `preservesPartitioning`:指出输入方法是否保留partitioner,默认是false,除非RDD的输入方法不能修改k值
   */
  @DeveloperApi
  def mapPartitionsWithContext[U: ClassTag](
      f: (TaskContext, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => f(context, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *  在RDD的每个Partitions上都执行这个方法,是mapPartitions的延伸的,可以使用taskContext关闭
   */
  @deprecated("use mapPartitionsWithIndex", "0.7.0")
  def mapPartitionsWithSplit[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    mapPartitionsWithIndex(f, preservesPartitioning)
  }

  /**
   * 在RDD上执行f函数,f有一个额外的参数,即A.这个额外的参数将被constructA构造出来,在
   * 指定索引的每个partition上is called(称为/调用)
   */
  @deprecated("use mapPartitionsWithIndex", "1.0.0")
  def mapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => U): RDD[U] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.map(t => f(t, a))
    }, preservesPartitioning)
  }

  /**
   * 类似flatmap,但是这个函数引入了partition的索引.
   */
  @deprecated("use mapPartitionsWithIndex and flatMap", "1.0.0")
  def flatMapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => Seq[U]): RDD[U] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.flatMap(t => f(t, a))
    }, preservesPartitioning)
  }

  /**
   * 和forachPartition类似,第一个参数是partition的索引号,第二个参数是对应partition对应的data
   * 对RDD的每个元素,都使用f
   */
  @deprecated("use mapPartitionsWithIndex and foreach", "1.0.0")
  def foreachWith[A](constructA: Int => A)(f: (T, A) => Unit) {
    mapPartitionsWithIndex { (index, iter) =>
      val a = constructA(index)
      iter.map(t => {f(t, a); t})
    }.foreach(_ => {})
  }

  /**
   * 使用p过滤RDD
   * 是filterwith的扩展版本,第一个参数是Int->T的形式,其中
   * Int代表的是partition的索引,T代表你要转换成的类型,第二个参数是(U,T)->boolean的形式,T是partition的索引,U是值
   */
  @deprecated("use mapPartitionsWithIndex and filter", "1.0.0")
  def filterWith[A](constructA: Int => A)(p: (T, A) => Boolean): RDD[T] = {
    mapPartitionsWithIndex((index, iter) => {
      val a = constructA(index)
      iter.filter(t => p(t, a))
    }, preservesPartitioning = true)
  }

  /**
   *
   * 将两个RDD合成一个k-v类型的RDD.要求两个RDD,有相同的partition,每个partition有相同的元素.
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next = (thisIter.next, otherIter.next)
      }
    }
  }

  /**
   *
   * 类似于zip,但是提供了更多的控制. (对一个或者多个RDD的partition进行zip,运行f后返回一个新的RDD)
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)

  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, false)

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, false)

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, false)


  /** ************************************************************************************
    * 下面是action
    ***************************************************************************************/

  /**
   * 对RDD中每一个element都执行这个方法
   */
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   *对RDD中每一个partition都执行这个方法
   */
  def foreachPartition(f: Iterator[T] => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
   * 将RDD中的元素转换成数组
   */
  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
   *返回一个保所RDD中所有元素的迭代器
   *
   * 这个迭代器在这个该RDD的最大partition中将花费更大的内存
   *
   */
  def toLocalIterator: Iterator[T] = {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p), allowLocal = false).head
    }
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * 返回RDD中包含所有元素的数组
   */
  @deprecated("use collect", "1.0.0")
  def toArray(): Array[T] = collect()

  /**
   * 将一个RDD中的所有值都运行f,并和指定值进行匹配,成功后返回一个新的RDD
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = {
    filter(f.isDefinedAt).map(f)
  }

  /**
   * 减法:RDD1-RDD2=返回和RDD1相比,RDD2中没有的
   */
  def subtract(other: RDD[T]): RDD[T] =
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.size)))

  /**
   * 减法:RDD1-RDD2=返回和RDD1相比,RDD2中没有的
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] =
    subtract(other, new HashPartitioner(numPartitions))

  /**
   * 减法:RDD1-RDD2=返回和RDD1相比,RDD2中没有的
   */
  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions = p.numPartitions
        override def getPartition(k: Any) = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   *
   * 二元操作符,对RDD中的元素执行Reduces操作.非常常用的命令,但是在参数中的方法,一定是迭代类型的
   */
  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   *
   * 把所有data的值都聚合在一起,每个partition的值初始化为zeroValue的值.该方法的op(t1,t2)允许修改t1,之后作为结果值
   * 返回,避免对象关联,然后,不能修改t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    //复制zerovalue,也将序列化他作为任务的一部分
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
   * 函数有三个入参,一是初始值ZeroValue,二是seqOp,三为combOp.
   * Aggregate每个partition的元素,之后把所有partition的结果,用于combine函数,和ZeroValue.这个函数返回一个不同的
   * 结果类型U.我们需要使用一个操作符对U中的T进行排序,使用scala.TraversableOnce.方法是允许修改,
   * 并且返回第一个参数代替创建U,避免内存分配.
   *
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * :: Experimental ::
   * Count()方法的近似,在指定事件内,返回一个可能不完整的结果,尽管所有的任务已经完成
   */
  @Experimental
  def countApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.size, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   *
   * 返回RDD每个唯一值的计数,返回形式是k-v形式(value,count).最后的combine步骤发生在本地的master上,
   * 相当于运行reduce任务
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValue() does not support arrays")
    }
    // TODO: This should perhaps be distributed by default.
    val countPartition = (iter: Iterator[T]) => {
      val map = new OpenHashMap[T,Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      Iterator(map)
    }: Iterator[OpenHashMap[T,Long]]
    val mergeMaps = (m1: OpenHashMap[T,Long], m2: OpenHashMap[T,Long]) => {
      m2.foreach { case (key, value) =>
        m1.changeValue(key, value, _ + value)
      }
      m1
    }: OpenHashMap[T,Long]
    val myResult = mapPartitions(countPartition).reduce(mergeMaps)
    //转换成scala的不可变map
    val mutableResult = scala.collection.mutable.Map[T,Long]()
    myResult.foreach { case (k, v) => mutableResult.put(k, v) }
    mutableResult
  }

  /**
   * :: Experimental ::
   * countByValue的近似版本
   */
  @Experimental
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
      : PartialResult[Map[T, BoundedDouble]] =
  {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T,Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T,Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.size, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * 返回RDD中去重后的element
   *
   * 这个算法基于streamlib的 "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * 时间复杂度:`1.054 / sqrt(2^p)`.设置非0的`sp > p`, would trigger sparse representation of registers
   * 将会减少内存的计算量并且提高精度,当基数很小的时候.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  @Experimental
  def countApproxDistinct(p: Int, sp: Int): Long = {
    require(p >= 4, s"p ($p) must be greater than 0")
    require(sp <= 32, s"sp ($sp) cannot be greater than 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * 返回RDD去重后元素个数的近似
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   *
   * 参数1_relativeSD:相对精度,较小的值创建计数器,但需要更大的空间,必须大于0.000017
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = {
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(p, 0)
  }

  /**
   *根据RDD元素的indices,进行zip操作.首先基于partition索引排序,然后在每个partition排序.所以第一个partition
   * 的索引是0.这个和scala的zipWithIndex()方法很相似,但是他对索引类型使用long代替了int.
   * 当这个RDD包含超过一个partition的时候,这个方法会触发Spark的job
   */
  def zipWithIndex(): RDD[(T, Long)] = new ZippedWithIndexRDD(this)

  /**
   *
   * 根据RDD生成的id进行zip操作.Items是第k个partition,将得到k,n+k,2*n+k...........n是partition的数量.
   * 所以存在很多差异,且这个方法不会触发Spark的job.和[[org.apache.spark.rdd.RDD#zipWithIndex]]是不同的
   *
   */
  def zipWithUniqueId(): RDD[(T, Long)] = {
    val n = this.partitions.size.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      iter.zipWithIndex.map { case (item, i) =>
        (item, i * n + k)
      }
    }
  }

  /**
   *抽取n个items,并且返回一个数组.首先扫描第一个partition,然后使用该partition的结果估计所需要的额外partition的数量,
   * 以满足要求
   *
   */
  def take(num: Int): Array[T] = {
    if (num == 0) {
      return new Array[T](0)
    }

    val buf = new ArrayBuffer[T]
    val totalParts = this.partitions.length
    var partsScanned = 0
    while (buf.size < num && partsScanned < totalParts) {
      //迭代次数是partition的数量.可能这个数字要比totalParts大,因为在runjob的时候,我们实际上会在totalParts上会有
      //重叠
      var numPartsToTry = 1
      if (partsScanned > 0) {
        //如果你不能找到第一次迭代之后的rows,仅仅尝试next.
        //另外,我们需要尝试插入partition的值,但是不能超过50%
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * num * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = num - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p, allowLocal = true)

      res.foreach(buf ++= _.take(num - buf.size))
      partsScanned += numPartsToTry
    }

    buf.toArray
  }

  /**
   * 返回RDD的第一个partition
   */
  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  /**
   * 返回我们指定的隐式排序条件下,RDD的最大的top k个元素.这个操作和[[takeOrdered]]正好相反.
   * 例如:
   *
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * 参数1_num:返回元素的个数
   * 参数2_ord:隐式排序
   * 返回:top elements组成的数组
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = takeOrdered(num)(ord.reverse)

  /**
   * 返回我们指定的隐式排序条件下,RDD的最大的top k个元素.这个操作和[[top]]正好相反.
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * 参数1_num:返回元素的个数
   * 参数2_ord:隐式排序
   * 返回:top elements组成的数组
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    mapPartitions { items =>
      // Priority keeps the largest elements, so let's reverse the ordering.
      val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
      queue ++= util.collection.Utils.takeOrdered(items, num)(ord)
      Iterator.single(queue)
    }.reduce { (queue1, queue2) =>
      queue1 ++= queue2
      queue1
    }.toArray.sorted(ord)
  }

  /**
   * 返回按照隐式排序中,RDD中的最大者
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = this.reduce(ord.max)

  /**
   * 返回按照隐式排序中,RDD中的最小者.
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = this.reduce(ord.min)

  /**
   * 把RDD存到text文件中,使用字符串代表element
   */
  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * 把RDD存到压缩的text文件中,使用字符串代表element
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }

  /**
   * 把RDD存储到一个系列化的Seq序列中
   */
  def saveAsObjectFile(path: String) {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
   * 创建RDD的一个elements的元组,通过使用f函数
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = {
    map(x => (f(x), x))
  }

  /**
    * 一个用于测试的私有方法,查看每个partition的内容
    * */
  private[spark] def collectPartitions(): Array[Array[T]] = {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * 对该RDD做一个检查点.其将保存为一个文件在SparkContext.setCheckpoiintDir()设置的检查点目录中,
   * 并且所有的父依赖RDD都将被删除.这个方法一定在该RDD执行的时候调用.推荐持久化RDD到内存中,另外存储到
   * 文件中也将重新计算.
   */
  def checkpoint() {
    if (context.checkpointDir.isEmpty) {
      throw new Exception("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new RDDCheckpointData(this))
      checkpointData.get.markForCheckpoint()
    }
  }

  /**
   * 返回RDD是否被checkpoint了
   */
  def isCheckpointed: Boolean = checkpointData.exists(_.isCheckpointed)

  /**
   * 得到该RDD的checkpoint时,文件的名字
   */
  def getCheckpointFile: Option[String] = checkpointData.flatMap(_.getCheckpointFile)

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /**使用code创建这个RDD(例如:'textFile','parallelize')
    * */
  @transient private[spark] val creationSite = Utils.getCallSite
  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  /** 返回第一个父RDD*/
  protected[spark] def firstParent[U: ClassTag] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** 创建这个RDD的SparkContext
    * */
  def context = sc

  /**
   * 改变RDD的ClassTag的私有API
   *用于内部java<->scala API的兼容性
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * 改变RDD的ClassTag的私有API
   *用于内部java<->scala API的兼容性
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  /**
   * 对该RDD执行checkpoint.使用这个RDD参与的job已经完成之后调用(所以这个RDD已经物化,并且可能存储在内容中)
   * doCheckpoint()可能在父RDD中递归的调用.
   *
   */
  private[spark] def doCheckpoint() {
    if (!doCheckpointCalled) {
      doCheckpointCalled = true
      if (checkpointData.isDefined) {
        checkpointData.get.doCheckpoint()
      } else {
        dependencies.foreach(_.rdd.doCheckpoint())
      }
    }
  }

  /**
   * 从checkpoint读取到的RDD,已经改变了和原始父RDD之间的依赖关系,忘记了原来的依赖和partitions
   *
   */
  private[spark] def markCheckpointed(checkpointRDD: RDD[_]) {
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
   *
   * 清理掉这个RDD的依赖关系.这个方法必须确定所有的依赖的父RDD都被删除,并且保证父RDD能够被gc
   * RDD的子类可能会覆写实现这个方法,自己的方式去清理.参见:[[org.apache.spark.rdd.UnionRDD]]看看例子
   *
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /**使用debugging来描述RDD的递归调用关系,很常用
    * */
  def toDebugString: String = {
    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_,_,_]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_,_,_]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_,_,_]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    //第一个RDD没有依赖的父RDD,所以不需要 a+-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.size + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))
      Seq(partitionStr + " " + rdd) ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.size + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))
      Seq(thisPrefix + "+-" + partitionStr + " " + rdd) ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(rdd: RDD[_],
                    prefix: String = "",
                    isShuffle: Boolean = true,
                    isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      }
      else {
        Seq(prefix + rdd) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}
