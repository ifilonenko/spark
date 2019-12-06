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
package org.apache.spark.scheduler.cluster.k8s

import java.util

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[spark] class ExecutorPodControllerImpl(
    val conf: SparkConf)
  extends ExecutorPodController {

  private var kubernetesClient: KubernetesClient = _

  private var numAdded: Int = _

  override def initialize(kClient: KubernetesClient) : Unit = {
    kubernetesClient = kClient
    numAdded = 0
  }
  override def addPod(pod: Pod): Unit = {
    kubernetesClient.pods().create(pod)
    synchronized {
      numAdded += 1
    }
  }

  override def commitAndGetTotalAllocated(): Int = {
    val totalNumAdded = numAdded
    synchronized {
      numAdded = 0
    }
    totalNumAdded
  }

  override def removePod(pod: Pod): Unit = {
    // If deletion failed on a previous try, we can try again if resync informs us the pod
    // is still around.
    // Delete as best attempt - duplicate deletes will throw an exception but the end state
    // of getting rid of the pod is what matters.
    Utils.tryLogNonFatalError {
      kubernetesClient
        .pods()
        .withName(pod.getMetadata.getName)
        .delete()
    }
  }

  override def removePods(pods: util.List[Pod]): Boolean = {
    kubernetesClient.pods().delete(pods)
  }
}
