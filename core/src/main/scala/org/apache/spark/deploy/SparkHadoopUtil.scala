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

package org.apache.spark.deploy

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{Logging, SparkContext, SparkException}

import scala.collection.JavaConversions._

/**
 *
 *包含Spark对Hadoop操作的一些方法
 */
class SparkHadoopUtil extends Logging {
  val conf: Configuration = newConfiguration()
  UserGroupInformation.setConfiguration(conf)

  /**
   * 运行给定的函数作为一个本地线程变量(分配给子线程),被用来进行HDFS和YARN验证
   *
   * 注意:如果这个函数在统一进程中重复使用,你需要看:https://issues.apache.org/jira/browse/HDFS-3545
   * 可能执行FileSystem.closeAllForUGI避免文件系统溢出
   */
  def runAsSparkUser(func: () => Unit) {
    val user = Option(System.getenv("SPARK_USER")).getOrElse(SparkContext.SPARK_UNKNOWN_USER)
    if (user != SparkContext.SPARK_UNKNOWN_USER) {
      logDebug("running as user: " + user)
      val ugi = UserGroupInformation.createRemoteUser(user)
      transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        def run: Unit = func()
      })
    } else {
      logDebug("running as SPARK_UNKNOWN_USER")
      func()
    }
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    for (token <- source.getTokens()) {
      dest.addToken(token)
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
   *
   * 返回一个合适的(子类)的配置.创建config来初始化hadoop的subsystems
   */
  def newConfiguration(): Configuration = new Configuration()

  /**
   *添加一个用户可信认证给job是重要的,为了保护运行Hadoop集群
   *
   */
  def addCredentials(conf: JobConf) {}

  def isYarnMode(): Boolean = { false }

  def getCurrentUserCredentials(): Credentials = { null }

  def addCurrentUserCredentials(creds: Credentials) {}

  def addSecretKeyToUserCredentials(key: String, secret: String) {}

  def getSecretKeyFromUserCredentials(key: String): Array[Byte] = { null }

  def loginUserFromKeytab(principalName: String, keytabFilename: String) { 
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
  }

}

object SparkHadoopUtil {

  private val hadoop = {
    val yarnMode = java.lang.Boolean.valueOf(
        System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (yarnMode) {
      try {
        Class.forName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil")
          .newInstance()
          .asInstanceOf[SparkHadoopUtil]
      } catch {
       case e: Exception => throw new SparkException("Unable to load YARN support", e)
      }
    } else {
      new SparkHadoopUtil
    }
  }

  def get: SparkHadoopUtil = {
    hadoop
  }
}
