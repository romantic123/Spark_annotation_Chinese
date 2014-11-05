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

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{ThreadFactory, ConcurrentHashMap, Executors, ThreadPoolExecutor}

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}

import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.json4s._
import tachyon.client.{TachyonFile,TachyonFS}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.ExecutorUncaughtExceptionHandler
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

  /** CallSite代表用户代码的一个位置,他可能很短也可能很长. */
private[spark] case class CallSite(shortForm: String, longForm: String)

/**
 *Spark的各种实用方法
 */
private[spark] object Utils extends Logging {
  val random = new Random()

  def sparkBin(sparkHome: String, which: String): File = {
    val suffix = if (isWindows) ".cmd" else ""
    new File(sparkHome + File.separator + "bin", which + suffix)
  }

  /** 使用java的序列化器序列化对象 */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** 使用java的序列化器描述一个对*/
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /**使用java的序列化器(Java serialization)和类加载器(ClassLoader)描述一个Object  */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

  /**  描述一个Long的值(used for {@link org.apache.spark.api.python.PythonPartitioner}) */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    //注意:我们假设我们有一个long值编码在网络字节中
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /**  通过使用特定的序列化器来进行序列化
    * */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit) = {
    val osWrapper = ser.serializeStream(new OutputStream {
      def write(b: Int) = os.write(b)

      override def write(b: Array[Byte], off: Int, len: Int) = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /**通过使用特定的序列化器进行描述*/
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit) = {
    val isWrapper = ser.deserializeStream(new InputStream {
      def read(): Int = is.read()

      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
   * 得到类加载器(ClassLoader)加载的火花
   */
  def getSparkClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * 得到这个线程Context加载器,如果不存在,类的加载器加载Spark
   * 当设置ClassLoader的chains时,用此方法找到一个当前激活的loader
   *
   */
  def getContextOrSparkClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** 确定当前线程提供的class是否可以加载 */
  def classIsLoadable(clazz: String): Boolean = {
    Try { Class.forName(clazz, false, getContextOrSparkClassLoader) }.isSuccess
  }

  /** 装载className指定的类
    * */
  def classForName(className: String) = Class.forName(className, true, getContextOrSparkClassLoader)

  /**
   * Primitive often used when writing {@link java.nio.ByteBuffer} to {@link java.io.DataOutput}.
   */
  def writeByteBuffer(bb: ByteBuffer, out: ObjectOutput) = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /** 将字符串拆分为非字幕字符 */
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j)
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    buf
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()
  private val shutdownDeleteTachyonPaths = new scala.collection.mutable.HashSet[String]()

  //通过关闭hook注册将要删除的路径
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  //通过关闭hook来关闭将要删除的tachyon路径
  def registerShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths += absolutePath
    }
  }

  //通过关闭hook,这个注册的路径被删除了嘛?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  //通过关闭hook,这个注册的路径被删除了嘛?
  def hasShutdownDeleteTachyonDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  //注意:如果文件是注册路径的下的子文件,当和其不等的时候,返回true,相等的时候,返回false.
  //这个方法用来确保关闭两个hooks,不要尝试删除各自的路径.导致IO异常和清理不完全
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  //注意:如果文件是注册路径的下的子文件,当和其不等的时候,返回true,相等的时候,返回false.
  //这个方法用来确保关闭两个hooks,不要尝试删除各自的路径.导致IO异常和清理不完全
  def hasRootAsShutdownDeleteDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    val retval = shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /**在给定的父目录下创建一个临时目录*/
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }

    registerShutdownDeleteDir(dir)

    //添加一个shutdown hook,当JVM退出时来删除临时文件路径
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        //如果一些补丁的上级还没有注册就删除之
        if (! hasRootAsShutdownDeleteDir(dir)) Utils.deleteRecursively(dir)
      }
    })
    dir
  }

  /**  从所有的InputStream和OutStream中复制所有的数据 */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false): Long =
  {
    var count = 0L
    try {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val size = inChannel.size()

        // In case transferTo method transferred less data than we have required.
        while (count < size) {
          count += inChannel.transferTo(count, size - count, outChannel)
        }
      } else {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
      }
      count
    } finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  /**
   *构造一个URI容器信息,用于身份认证.
   * 这也是默认的身份验证,基于URI的user/password
   * 注意:这依赖于Authenticator.setDefault正确解码用户名/密码.这也是目前的SecurityManager设置
   *
   */
  def constructURIForAuthentication(uri: URI, securityMgr: SecurityManager): URI = {
    val userCred = securityMgr.getSecretKey()
    if (userCred == null) throw new Exception("Secret key is null with authentication on")
    val userInfo = securityMgr.getHttpUser()  + ":" + userCred
    new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(),
      uri.getQuery(), uri.getFragment())
  }

  /**
   * 下载executor所需要的文件,支持多种方式获取文件.包括基于URL的HTTP,HDFS,本地文件系统
   * 如果目标文件已经存在或者和请求的内容有不同,就会抛出异常
   *
   */
  def fetchFile(url: String, targetDir: File, conf: SparkConf, securityMgr: SecurityManager) {
    val filename = url.split("/").last
    val tempDir = getLocalDir(conf)
    val tempFile =  File.createTempFile("fetchFileTemp", null, new File(tempDir))
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", false)
    uri.getScheme match {
      case "http" | "https" | "ftp" =>
        logInfo("Fetching " + url + " to " + tempFile)

        var uc: URLConnection = null
        if (securityMgr.isAuthenticationEnabled()) {
          logDebug("fetchFile with security enabled")
          val newuri = constructURIForAuthentication(uri, securityMgr)
          uc = newuri.toURL().openConnection()
          uc.setAllowUserInteraction(false)
        } else {
          logDebug("fetchFile not using security")
          uc = new URL(url).openConnection()
        }

        val timeout = conf.getInt("spark.files.fetchTimeout", 60) * 1000
        uc.setConnectTimeout(timeout)
        uc.setReadTimeout(timeout)
        uc.connect()
        val in = uc.getInputStream()
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          if (fileOverwrite) {
            targetFile.delete()
            logInfo(("File %s exists and does not match contents of %s, " +
              "replacing it with %s").format(targetFile, url, url))
          } else {
            tempFile.delete()
            throw new SparkException(
              "File " + targetFile + " exists and does not match contents of" + " " + url)
          }
        }
        Files.move(tempFile, targetFile)
      case "file" | null =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(url)
        var shouldCopy = true
        if (targetFile.exists) {
          if (!Files.equal(sourceFile, targetFile)) {
            if (fileOverwrite) {
              targetFile.delete()
              logInfo(("File %s exists and does not match contents of %s, " +
                "replacing it with %s").format(targetFile, url, url))
            } else {
              throw new SparkException(
                "File " + targetFile + " exists and does not match contents of" + " " + url)
            }
          } else {
            // Do nothing if the file contents are the same, i.e. this file has been copied
            // previously.
            logInfo(sourceFile.getAbsolutePath + " has been previously copied to "
              + targetFile.getAbsolutePath)
            shouldCopy = false
          }
        }

        if (shouldCopy) {
          // The file does not exist in the target directory. Copy it there.
          logInfo("Copying " + sourceFile.getAbsolutePath + " to " + targetFile.getAbsolutePath)
          Files.copy(sourceFile, targetFile)
        }
      case _ =>
        // Use the Hadoop filesystem library, which supports file://, hdfs://, s3://, and others
        val fs = getHadoopFileSystem(uri)
        val in = fs.open(new Path(uri))
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          if (fileOverwrite) {
            targetFile.delete()
            logInfo(("File %s exists and does not match contents of %s, " +
              "replacing it with %s").format(targetFile, url, url))
          } else {
            tempFile.delete()
            throw new SparkException(
              "File " + targetFile + " exists and does not match contents of" + " " + url)
          }
        }
        Files.move(tempFile, targetFile)
    }
    // Decompress the file if it's a .tar or .tar.gz
    if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xzf", filename), targetDir)
    } else if (filename.endsWith(".tar")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xf", filename), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")
  }

  /**
   *
   * 得到一个临时目录的路径.Spark的本地目录可以通过多种配置进行设置,但是有如下优先级:
   *  -如果是YARN容易的调用,将返回YARN选择的目录进行返回
   *  -如果是SPARK_LOCAL_DIRS环境变量进行设置的话,将返回他的一个目录.
   *  -如果是spark.lcoal.dir进行设置的话,将返回他的目录
   *  -否则就返回java.io.tmpdir.
   * 其中的一些配置选项可能是多条路径的集合,但是这个方法肯定返回一个单一目录.
   *
   *
   */
  def getLocalDir(conf: SparkConf): String = {
    getOrCreateLocalRootDirs(conf)(0)
  }

  private[spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    //YARN设置的环境变量
    //对于Hadoop 0.23.X,我们检查YARN_LOCAL_DIRS(我们使用下面的getYarnLocalDirs())
    //对于Hadoop2.x,我们检查CONTAINER_ID
    conf.getenv("CONTAINER_ID") != null || conf.getenv("YARN_LOCAL_DIRS") != null
  }

  /**
   * 得到或者创建spark.local.dir or SPARK_LOCAL_DIRS指定的目录,并且返回只存在/可以创建的目录
   * 如果没有目录能被创建,将返回空集合
   *
   */
  private[spark] def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    val confValue = if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available.
      getYarnLocalDirs(conf)
    } else {
      Option(conf.getenv("SPARK_LOCAL_DIRS")).getOrElse(
        conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")))
    }
    val rootDirs = confValue.split(',')
    logDebug(s"Getting/creating local root dirs at '$confValue'")

    rootDirs.flatMap { rootDir =>
      val localDir: File = new File(rootDir)
      val foundLocalDir = localDir.exists || localDir.mkdirs()
      if (!foundLocalDir) {
        logError(s"Failed to create local root dir in $rootDir.  Ignoring this directory.")
        None
      } else {
        Some(rootDir)
      }
    }
  }

  /**  得到YARN的approved目录
    * */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(conf.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(conf.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  /**
   *将集合中的元素进行shuffle操作(随机排序),返回一个新的集合.与scala.util.Random.shuffle的操作
   * 不同,这个方法用一个生成一个本地的随机数,避免了线程竞争.
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * 对数组中的元素进行shuffle(随机排序),修改了原始的数组,返回原始的数组.....
   *
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * 得到本地host的ip
   * 注意:这个方法通常不再Spark的内部使用.
   */
  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        //地址解析成襄127.0.1.1,发生在Debian中,尽量使用本地网络接口,尝试找到一个更好的地址.
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
               !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            //我们找到了一个地址,看上去合理.
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  /**
   * 允许设置自定义的主机名,因为当我们在Mesos上运行的时候,我们需要用我们master相同的hostname
   */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * 得到本地机器的hostname
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  //通常情况下,这就是集群中节点的数目
  //如果不是,我们应该改变LRUCache的设置
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String,  Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    val indx: Int = hostPort.lastIndexOf(':')
    //这个地方还不太好,当我们处理IPV6的地址的时,Hadoop现在还不支持ipv6
    //现在我们认为如果端口存在,且是有效的,就不用检查其是否>0
    if (-1 == indx) {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      return retval
    }

    val retval = (hostPort.substring(0, indx).trim(), hostPort.substring(indx + 1).trim().toInt)
    hostPortParseResults.putIfAbsent(hostPort, retval)
    hostPortParseResults.get(hostPort)
  }

  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)

  /**
   * 创建一个有前缀线程名的线程工厂,并且设置线程的守护进程.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    daemonThreadFactoryBuilder.setNameFormat(prefix + "-%d").build()
  }

  /**
   * 包装了 newCachedThreadPool.线程的名字是prefix-ID的格式,ID的唯一的,按顺序分配的整数
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
  包装了 newCachedThreadPool.线程的名字是prefix-ID的格式,ID的唯一的,按顺序分配的整数
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 返回一个字符串,告诉已经过去多长时间了,以毫秒为单位.
   *
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  private def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) {
      throw new IOException("Failed to list files for dir: " + file)
    }
    files
  }

  /**
   * 递归的删除一个文件目录及其内容
   * Don't follow directories if they are symlinks.
   */
  def deleteRecursively(file: File) {
    if (file != null) {
      if ((file.isDirectory) && !isSymlink(file)) {
        for (child <- listFilesSafely(file)) {
          deleteRecursively(child)
        }
      }
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) {
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
        }
      }
    }
  }

  /**
   * 递归的删除一个文件目录及其内容
   */
  def deleteRecursively(dir: TachyonFile, client: TachyonFS) {
    if (!client.delete(dir.getPath(), true)) {
      throw new IOException("Failed to delete the tachyon dir: " + dir)
    }
  }

  /**
   * 查看文件的符号链接.
   */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (isWindows) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    if (fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())) {
      return false
    } else {
      return true
    }
  }

  /**
   * 发现所有文件里面最后的修改事件比你设置阈值最早的那些都将返回
   *
   * 参数1_dir:一定是一个目录的路径,或者抛出的异常参数.
   * 参数2_cutoff:毫秒,比这个早的都返回
   *
   */
  def findOldFiles(dir: File, cutoff: Long): Seq[File] = {
    val currentTimeMillis = System.currentTimeMillis
    if (dir.isDirectory) {
      val files = listFilesSafely(dir)
      files.filter { file => file.lastModified < (currentTimeMillis - cutoff * 1000) }
    } else {
      throw new IllegalArgumentException(dir + " is not a directory!")
    }
  }

  /**
   *JVM的内存设置,通过-Xmx(such as 300m or 1g)达到设置内存大小的目的.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }

  /**
   * 将字节数转换成人可读的大小:例如:4.0MB
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * 返回一个可读的时间:例如:.35ms
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }

  /**
   * 将字节数转换成人可读的大小:例如:4.0MB
   */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * 在给定的工作目录,执行一个命令,如果完成后的退出码不是0就抛出异常
   */
  def execute(command: Seq[String], workingDir: File) {
    val process = new ProcessBuilder(command: _*)
        .directory(workingDir)
        .redirectErrorStream(true)
        .start()
    new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
  }

  /**
     在给定的工作目录,执行一个命令,如果完成后的退出码不是0就抛出异常
   */
  def execute(command: Seq[String]) {
    execute(command, new File("."))
  }

  /**
   * 执行一个命令,得到输出解过,退出码非0就报错
   */
  def executeAndGetOutput(command: Seq[String], workingDir: File = new File("."),
                          extraEnvironment: Map[String, String] = Map.empty): String = {
    val builder = new ProcessBuilder(command: _*)
        .directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    new Thread("read stderr for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val output = new StringBuffer
    val stdoutThread = new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          output.append(line)
        }
      }
    }
    stdoutThread.start()
    val exitCode = process.waitFor()
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
    output.toString
  }

  /**
   *
   * 执行一块代码,进行单元测试,默认UncaughtExceptionHandler将forwarding任何未捕获的异常
   */
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case t: Throwable => ExecutorUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * 一个正则方法,用来匹配Spark的核心API,在我们想跳过某个方法的调用
   */
  private val SPARK_CLASS_REGEX = """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?\.[A-Z]""".r

  /**
   * 当调用Spark包中的类的时候,返回用户代码类中调用Spark类的类的名称,也包括调用的Spark方法.
   * 这个方法用于告诉用户,他们他们的代码在RDD中创建了什么.
   */
  def getCallSite: CallSite = {
    val trace = Thread.currentThread.getStackTrace()
      .filterNot { ste:StackTraceElement =>
      //当运行一些profilers的时候,当前的堆栈可能包含一些bogus frames.他确保了我们不会崩溃,在这些情况下
      //我们不能忽略任何检查
        (ste == null || ste.getMethodName == null || ste.getMethodName.contains("getStackTrace"))
      }

    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    var callStack = new ArrayBuffer[String]() :+ "<unknown>"

    for (el <- trace) {
      if (insideSpark) {
        if (SPARK_CLASS_REGEX.findFirstIn(el.getClassName).isDefined) {
          lastSparkMethod = if (el.getMethodName == "<init>") {
            // Spark method is a constructor; get its class name
            el.getClassName.substring(el.getClassName.lastIndexOf('.') + 1)
          } else {
            el.getMethodName
          }
          callStack(0) = el.toString // Put last Spark method on top of the stack trace.
        } else {
          firstUserLine = el.getLineNumber
          firstUserFile = el.getFileName
          callStack += el.toString
          insideSpark = false
        }
      } else {
        callStack += el.toString
      }
    }
    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    CallSite(
      shortForm = "%s at %s:%s".format(lastSparkMethod, firstUserFile, firstUserLine),
      longForm = callStack.take(callStackDepth).mkString("\n"))
  }

  /**我们指定一个start和end,返回一个文件中,从start到end的所有字符串
    * */
  def offsetBytes(path: String, start: Long, end: Long): String = {
    val file = new File(path)
    val length = file.length()
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = new FileInputStream(file)

    try {
      stream.skip(effectiveStart)
      stream.read(buff)
    } finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  /**
   * 返回一个string,是从一系列的的file里面获取的.`startIndex`和`endIndex`是从按照顺序文件
   * 累计获取的.
   */
  def offsetBytes(files: Seq[File], start: Long, end: Long): String = {
    val fileLengths = files.map { _.length }
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = files.zip(fileLengths).toMap
    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    for (file <- files) {
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file, " +
        s"with start index = $startIndexOfFile, end index = $endIndex")

      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */

      if (startIndex <= startIndexOfFile  && endIndex >= endIndexOfFile) {
        // Case C: 读取整个文件
        stringBuffer.append(offsetBytes(file.getAbsolutePath, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}}")
    }
    stringBuffer.toString
  }

  /**
   * 使用Spark的序列化器来复制一个object
   */
  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  /**
   * 检查该线程是否可以执行 shutdown hook.如果当前线程正在运行shutdown hook将返回true,但是其他情况可能也会返回true
   * (e.g.如果System.exit被并发线程调用 ).
   * 目前,判断JVM是否关闭by Runtime#addShutdownHook throwing an IllegalStateException.
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  def isSpace(c: Char): Boolean = {
    " \t\r\n".indexOf(c) != -1
  }

  /**
   *切割Shell的命令行,达到String到arguments的转换,string:'a "b c" d'转换为三个参数:'a', 'b c' and 'd'.
   */
  def splitCommandString(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord() {
      buf += curWord.toString
      curWord.clear()
    }
    var i = 0
    while (i < s.length) {
      val nextChar = s.charAt(i)
      if (inDoubleQuote) {
        if (nextChar == '"') {
          inDoubleQuote = false
        } else if (nextChar == '\\') {
          if (i < s.length - 1) {
            // Append the next character directly, because only " and \ may be escaped in
            // double quotes after the shell's own expansion
            curWord.append(s.charAt(i + 1))
            i += 1
          }
        } else {
          curWord.append(nextChar)
        }
      } else if (inSingleQuote) {
        if (nextChar == '\'') {
          inSingleQuote = false
        } else {
          curWord.append(nextChar)
        }
        // Backslashes are not treated specially in single quotes
      } else if (nextChar == '"') {
        inWord = true
        inDoubleQuote = true
      } else if (nextChar == '\'') {
        inWord = true
        inSingleQuote = true
      } else if (!isSpace(nextChar)) {
        curWord.append(nextChar)
        inWord = true
      } else if (inWord && isSpace(nextChar)) {
        endWord()
        inWord = false
      }
      i += 1
    }
    if (inWord || inDoubleQuote || inSingleQuote) {
      endWord()
    }
    buf
  }

 /**
  *计算x的模 mod,如果x是负数, x % 也是负的
   * 所以方法返回:(x % mod) + mod
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // Handles idiosyncracies with hash (add more as required)
  def nonNegativeHash(obj: AnyRef): Int = {

    // Required ?
    if (obj eq null) return 0

    val hash = obj.hashCode
    // math.abs fails for Int.MinValue
    val hashAbs = if (Int.MinValue != hash) math.abs(hash) else 0

    // Nothing else to guard against ?
    hashAbs
  }

  /**
    * 返回系统属性的副本,迭代的时候是线程安全的
    * */
  def getSystemProperties(): Map[String, String] = {
    System.getProperties.clone().asInstanceOf[java.util.Properties].toMap[String, String]
  }

  /**
   *重复执行一个task,同时,他支持JVM JIT的优化
   */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
   * timeing方法基于迭代器,允许JVM JIT优化
   * 参数1_numIters:迭代次数
   * 参数2_f:执行
   */
  def timeIt(numIters: Int)(f: => Unit): Long = {
    val start = System.currentTimeMillis
    times(numIters)(f)
    System.currentTimeMillis - start
  }

  /**
   * 返回迭代次数
   */
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  /**
   *
   * 创建符号链接.java7,Files.createSymbolicLink
   * 对于java6支持,Supports windows by doing copy,everything else uses "ln -sf".
   *
   * 参数1_src:source的绝对路径
   * 参数2_dst:dst的相对路径
   *
   */
  def symlink(src: File, dst: File) {
    if (!src.isAbsolute()) {
      throw new IOException("Source must be absolute")
    }
    if (dst.isAbsolute()) {
      throw new IOException("Destination must be relative")
    }
    var cmdSuffix = ""
    val linkCmd = if (isWindows) {
      // refer to http://technet.microsoft.com/en-us/library/cc771254.aspx
      cmdSuffix = " /s /e /k /h /y /i"
      "cmd /c xcopy "
    } else {
      cmdSuffix = ""
      "ln -sf "
    }
    import scala.sys.process._
    (linkCmd + src.getAbsolutePath() + " " + dst.getPath() + cmdSuffix) lines_!
      ProcessLogger(line => (logInfo(line)))
  }


  /**
    * 返回给定对象的类名称,删除$符号
    * */
  def getFormattedClassName(obj: AnyRef) = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /** 返回一个option,其中JNothing转换为None*/
  def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  /**返回一个空的JSON对象 */
  def emptyJson = JObject(List[JField]())

  /**
   * 根据给定的路径,返回一个Hadoop文件系统
   */
  def getHadoopFileSystem(path: URI): FileSystem = {
    FileSystem.get(path, SparkHadoopUtil.get.newConfiguration())
  }

  /**
   *  根据给定的路径,返回一个Hadoop文件系统
   */
  def getHadoopFileSystem(path: String): FileSystem = {
    getHadoopFileSystem(new URI(path))
  }

  /**
   * 返回给定目录的文件绝对路径
   */
  def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
  }

  /**
   * 是否是windows操作系统
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * 匹配一个Windows驱动,只包含一个字母字符
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * 格式一个windows路径,这样可以安全的传递给URI
   *
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  /**
   * 显示当前Spark能否运行单元测试
   */
  def isTesting = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * 从一个路径中的得到目录
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * 指定进程终止的最大等待事件
   * 返回指定时候后,进程是否终止了
   */
  def waitForProcess(process: Process, timeoutMs: Long): Boolean = {
    var terminated = false
    val startTime = System.currentTimeMillis
    while (!terminated) {
      try {
        process.exitValue
        terminated = true
      } catch {
        case e: IllegalThreadStateException =>
          // Process not terminated yet
          if (System.currentTimeMillis - startTime > timeoutMs) {
            return false
          }
          Thread.sleep(100)
      }
    }
    true
  }

  /**
   * 等待进程结束后,返回进程的一个stderr
   * 如果及程序没有在指定时间内终止,就返回None
   *
   */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = Utils.waitForProcess(process, timeoutMs)
    if (terminated) {
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    } else {
      None
    }
  }

  /**
   * 执行给定的block,loggong和re-throwing任何未捕获的异常
   * 封装在一个运行的线程的代码中是非常有用的,确保这个线程会被打印,避免Throwable
   *
   *
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /**如果给定的异常非常的严重,参见: scala.util.control.NonFatal*/
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
        false
      case _ =>
        true
    }
  }

  /**
   *返回一个对输入文件结构良好的URI描述
   *如果提供的路径不包含scheme,或者一个相对路径,他将转换成为绝对路径,使用file://scheme
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    //在这个Windows,这个文件的分隔符是"//",这与URI的格式不一致
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path

    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }
    uri.getScheme match {
      case windowsDrive(d) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case null =>
        // Preserve fragments for HDFS file name substitution (denoted by "#")
        // For instance, in "abc.py#xyz.py", "xyz.py" is the name observed by the application
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case _ =>
        uri
    }
  }

  /** 处理一个以","分隔的路径 */
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /**  从一个逗号路径集合中返回非本地路径
    * */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val formattedPath = if (windows) formatWindowsPath(p) else p
        new URI(formattedPath).getScheme match {
          case windowsDrive(d) if windows => false
          case "local" | "file" | null => false
          case _ => true
        }
      }
    }
  }

  /**返回一个字符串表示的异常,包括推展跟踪*/
  def exceptionString(e: Exception): String = {
    if (e == null) "" else exceptionString(getFormattedClassName(e), e.getMessage, e.getStackTrace)
  }

  /** 返回一个字符串表示的异常,包括推展跟踪 */
  def exceptionString(
      className: String,
      description: String,
      stackTrace: Array[StackTraceElement]): String = {
    val desc = if (description == null) "" else description
    val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
    s"$className: $desc\n$st"
  }

  /**
   * 使用指定的SparkConf的一些列的java options转换所有的Spartk Properties设置
   */
  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean) = _ => true): Seq[String] = {
    conf.getAll
      .filter { case (k, _) => filterKey(k) }
      .map { case (k, v) => s"-D$k=$v" }
  }

  /**
   * 绑定到一个端口的默认数量
   */
  val portMaxRetries: Int = {
    if (sys.props.contains("spark.testing")) {
      // Set a higher number of retries for tests...
      sys.props.get("spark.ports.maxRetries").map(_.toInt).getOrElse(100)
    } else {
      Option(SparkEnv.get)
        .flatMap(_.conf.getOption("spark.ports.maxRetries"))
        .map(_.toInt)
        .getOrElse(16)
    }
  }

  /**
   *
   * 根据给定的port启动一个服务,后续的每次尝试,都会1+该端口(除非端口是0)
   *
   * 参数1_startPort:启动服务的初始化端口
   * 参数2_maxRetries:尝试的最大数量
   * 参数3_startService:在给定端口上,使用该方法启动服务
   * 这可能会造成java.net.BindException端口冲突异常.
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      serviceName: String = "",
      maxRetries: Int = portMaxRetries): (T, Int) = {
    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) startPort else (startPort + offset) % 65536
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * 判断当前要绑定的端口是否已经绑定了
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null && e.getMessage.contains("Address already in use")) {
          return true
        }
        isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

}

/**
 * 一个使用的类:重定向process的stderr或者stdout地址
 */
private[spark] class RedirectThread(in: InputStream, out: OutputStream, name: String)
  extends Thread(name) {

  setDaemon(true)
  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      val buf = new Array[Byte](1024)
      var len = in.read(buf)
      while (len != -1) {
        out.write(buf, 0, len)
        out.flush()
        len = in.read(buf)
      }
    }
  }
}
