/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bqhiveloader

import org.apache.log4j
import org.apache.log4j.Level
import org.apache.log4j.Level.{DEBUG, INFO, OFF, WARN}

object Util {
  val DefaultLayout = new log4j.PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n")
  val ConsoleAppender = new log4j.ConsoleAppender(DefaultLayout)

  def setDebug(logName: String): Unit = setLvl(logName, DEBUG)

  def setWarn(logName: String): Unit = setLvl(logName, WARN)

  def setOff(logName: String): Unit = setLvl(logName, OFF)

  def setLvl(logName: String, level: log4j.Level): Unit = {
    val logger = org.apache.log4j.Logger.getLogger(logName)
    configureLogger(logger, level)
  }

  def newLogger(name: String, level: log4j.Level = INFO): org.apache.log4j.Logger = {
    val logger = log4j.Logger.getLogger(name)
    configureLogger(logger, level)
    logger
  }

  def configureLogger(logger: log4j.Logger, level: log4j.Level): log4j.Logger = {
    logger.setLevel(level)
    logger
  }

  def configureLogging(): Unit = {
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    rootLogger.addAppender(ConsoleAppender)
    rootLogger.setLevel(Level.INFO)
  }

  def quietSparkLogs(): Unit = {
    Util.setDebug("com.google.cloud.example")
    Util.setWarn("org.apache.orc.impl.MemoryManagerImpl")
    Util.setWarn("org.apache.spark.network.netty")
    Util.setWarn("org.apache.spark.executor.Executor")
    Util.setWarn("org.apache.spark.scheduler")
    Util.setWarn("org.apache.spark.SparkEnv")
    Util.setWarn("org.apache.spark.sql.catalyst.expressions.codegen")
    Util.setWarn("org.apache.spark.sql.execution")
    Util.setWarn("org.apache.spark.sql.internal.SharedState")
    Util.setWarn("org.apache.spark.SecurityManager")
    Util.setWarn("org.apache.spark.storage.BlockManager")
    Util.setWarn("org.apache.spark.storage.BlockManagerInfo")
    Util.setWarn("org.apache.spark.storage.BlockManagerMaster")
    Util.setWarn("org.apache.spark.storage.BlockManagerMasterEndpoint")
    Util.setWarn("org.apache.spark.storage.DiskBlockManager")
    Util.setWarn("org.apache.spark.storage.memory.MemoryStore")
    Util.setWarn("org.apache.spark.ui")
    Util.setWarn("org.apache.spark.util")
    Util.setOff("org.apache.hadoop.util.NativeCodeLoader")
  }
}
