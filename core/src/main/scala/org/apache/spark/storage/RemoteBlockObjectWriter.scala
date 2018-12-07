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

import java.io._
import java.nio.channels.FileChannel

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.external.ShuffleWriteSupport
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class RemoteBlockObjectWriter(
    shuffleWriteSupport: ShuffleWriteSupport,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active BlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null) extends Logging {

  private var byteArrayOutput: ByteArrayOutputStream = null
  private var bs: OutputStream = null
  private var objectOutputStream: ObjectOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var bufferedOS: BufferedOutputStream = null
  private var serializationStream: SerializationStream = null
  private var mcOS: ManualCloseOutputStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  private def initialize(): Unit = {
    byteArrayOutput = new ByteArrayOutputStream()
    objectOutputStream = new ObjectOutputStream(byteArrayOutput)
    ts = new TimeTrackingOutputStream(writeMetrics, objectOutputStream)
    mcOS: ManualCloseOutputStream =
  }

  /**
    * Guards against close calls, e.g. from a wrapping stream.
    * Call manualClose to close the stream that was extended by this trait.
    * Commit uses this trait to close object streams without paying the
    * cost of closing and opening the underlying file.
    */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  /**
    * Keep track of number of records written and also use this to periodically
    * output bytes written since the latter is expensive to do for each record.
    * And we reset it after every commitAndGet called.
    */
  private var numRecordsWritten = 0

  def open(): RemoteBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    bs = serializerManager.wrapStream(blockId, new BufferedOutputStream(ts, bufferSize))
    serializationStream = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

}
