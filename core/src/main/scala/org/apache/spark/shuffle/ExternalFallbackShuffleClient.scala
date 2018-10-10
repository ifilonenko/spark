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

package org.apache.spark.shuffle

import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.shuffle._

private[spark] class ExternalFallbackShuffleClient(
    externalShuffleClient: ExternalShuffleClient,
    baseBlockTransferService: BlockTransferService) extends ShuffleClient {

  override def init(appId: String): Unit = {
    externalShuffleClient.init(appId)
    baseBlockTransferService.init(appId)
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      isBackup: Boolean,
      listener: BlockFetchingListener,
      downloadFileManager: DownloadFileManager): Unit = {
    if (isBackup) {
      externalShuffleClient.fetchBlocks(
        host,
        port,
        execId,
        blockIds,
        isBackup,
        listener,
        downloadFileManager)
    } else {
      baseBlockTransferService.fetchBlocks(
        host, port, execId, blockIds, isBackup, listener, downloadFileManager)
    }
  }

  override def close(): Unit = {
    baseBlockTransferService.close()
    externalShuffleClient.close()
  }

  def registerWithShuffleServerForBackups(
      host: String,
      port: Int,
      execId: String,
      shuffleManager: String) : Unit = {
    externalShuffleClient.registerWithShuffleServerForBackups(host, port, execId, shuffleManager)
  }
}
