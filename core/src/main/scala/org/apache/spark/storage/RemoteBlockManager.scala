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

import java.io.InputStream

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.external.ShuffleReadSupport

 /**
  * Creates and maintains the logical mapping between logical blocks and physical on-disk
  * locations. One block is mapped to one file with a name given by its BlockId.
  *
  * Block files are hashed among the directories listed in spark.local.dir (or in
  * SPARK_LOCAL_DIRS, if it's set).
  */
private[spark] class RemoteBlockManager(
    conf: SparkConf,
    shuffleReadSupport: ShuffleReadSupport)
  extends Logging with BlockMapper {

  def getInputStream(blockId: BlockId): InputStream = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        val reader = shuffleReadSupport.newPartitionReader(
          conf.getAppId, shufId, mapId)
        reader.fetchPartition(reduceId)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block")
    }
  }

   override def containsBlock(blockId: BlockId): Boolean =
     blockId match {
       case ShuffleBlockId(shufId, mapId, reduceId) =>
         val reader = shuffleReadSupport.newPartitionReader(
           conf.getAppId, shufId, mapId)
         reader.fetchPartition(reduceId).available() > 0
       case _ =>
         throw new SparkException(
           "Failed to get block " + blockId + ", which is not a shuffle block")
     }
 }