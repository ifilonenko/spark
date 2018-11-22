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

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.SettableFuture
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.spark.{MapOutputTracker, ReportBackedUpMapOutput}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, UploadShuffleFileStream, UploadShuffleIndexFileStream}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.scheduler.{MapStatus, RelocatedMapStatus}
import org.apache.spark.storage.BlockManagerId

class BackingUpShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    delegateWriter: ShuffleWriter[K, V],
    backupShuffleServiceClient: TransportClient,
    transportConf: TransportConf,
    mapOutputTracker: MapOutputTracker,
    backupExecutor: ExecutorService,
    backupHost: String,
    backupPort: Int,
    appId: String,
    execId: String,
    shuffleId: Int,
    mapId: Int)
  extends ShuffleWriter[K, V] with Logging {

  private implicit val backupExecutorContext = ExecutionContext.fromExecutorService(backupExecutor)

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    delegateWriter.write(records)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    val delegateMapStatus = delegateWriter.stop(success)
    delegateMapStatus.foreach { _ =>
      val outputFile = shuffleBlockResolver.getDataFile(shuffleId, mapId)
      val indexFile = shuffleBlockResolver.getIndexFile(shuffleId, mapId)
      if (outputFile.isFile && indexFile.isFile) {
        val uploadBackupFileRequest = new UploadShuffleFileStream(
          appId, execId, shuffleId, mapId)
        val uploadIndexFileRequest = new UploadShuffleIndexFileStream(
          appId, execId, shuffleId, mapId)

        val backupFileTask: Future[Unit] = Future {
          backupFile(outputFile, uploadBackupFileRequest)
          backupFile(indexFile, uploadIndexFileRequest)
        }

        backupFileTask.onComplete {
          case Success(_) =>
            val backedUpMapStatus = RelocatedMapStatus(
              delegateMapStatus.get,
              BlockManagerId(execId, backupHost, backupPort, None, isBackup = true))
            mapOutputTracker.trackerEndpoint.send(
              ReportBackedUpMapOutput(shuffleId, mapId, backedUpMapStatus))
          case Failure(_) => logError("An error has occured in backing up")
        }
      }
    }
    delegateMapStatus
  }

  private def backupFile(
      fileToBackUp: File,
      backupFileRequest: BlockTransferMessage) {
    val dataFileBuffer = new FileSegmentManagedBuffer(
      transportConf, fileToBackUp, 0, fileToBackUp.length())
    val uploadBackupRequestBuffer = new NioManagedBuffer(backupFileRequest.toByteBuffer)
    val awaitCompletion = SettableFuture.create[Boolean]
    backupShuffleServiceClient.uploadStream(
      uploadBackupRequestBuffer, dataFileBuffer, new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logInfo("Successfully backed up shuffle map data file" +
            s" (shuffle id: $shuffleId, map id: $mapId, executor id: $execId)")
          awaitCompletion.set(true)
        }

        /** Exception either propagated from server or raised on client side. */
        override def onFailure(e: Throwable): Unit = {
          logError("Failed to back up shuffle map data file" +
            s" (shuffle id: $shuffleId, map id: $mapId, executor id: $execId)")
          awaitCompletion.setException(e)
        }
      })
    awaitCompletion.get()
  }
}
