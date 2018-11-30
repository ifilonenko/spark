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

package org.apache.spark.shuffle.external.default

import java.io.{DataInputStream, File, InputStream, OutputStream}

import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.shuffle.external._

private[spark] object DefaultShuffleDataIO extends ShuffleDataIO {
  override def writeSupport(): ShuffleWriteSupport = {

    val shuffleWriteSupport = new ShuffleWriteSupport {
      override def newPartitionWriter(appId: String, shuffleId: Int, mapId: Int): ShufflePartitionWriter = ??

        new ShufflePartitionWriter {
          override def appendPartition(partitionId: Long, partitionInput: OutputStream): Unit = ???

          override def close(): Unit = ???
        }
    }
  }

  override def readSupport(): ShuffleReadSupport = new ShuffleReadSupport {
    override def newPartitionReader(appId: String, shuffleId: Int, mapId: Int): ShufflePartitionReader =
      new ShufflePartitionReader {
        override def fetchPartition(reduceId: Long): InputStream = ???

        override def getDataFile(shuffleId: Int, mapId: Int): File = ???

        override def getIndexFile(shuffleId: Int, mapId: Int): File = ???

        override def deleteDataFile(shuffleId: Int, mapId: Int): Unit = ???

        override def deleteIndexFile(shuffleId: Int, mapId: Int): Unit = ???
      }
  }
}
