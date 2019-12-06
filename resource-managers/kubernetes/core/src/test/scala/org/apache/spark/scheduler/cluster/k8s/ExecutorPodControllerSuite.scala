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

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Fabric8Aliases.PODS

class ExecutorPodControllerSuite extends SparkFunSuite with BeforeAndAfter {

  private var executorPodController: ExecutorPodController = _

  private val sparkConf = new SparkConf(false)

  private val execExampleId = "exec-id"

  private def buildPod(execId: String ): Pod = {
    new PodBuilder()
      .withNewMetadata()
      .withName(execId)
      .endMetadata()
      .build()
  }

  private val execPod = buildPod(execExampleId)

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var execPodOperations: PodResource[Pod, DoneablePod] = _


  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(execExampleId))
        .thenReturn(execPodOperations)
    executorPodController = new ExecutorPodControllerImpl(sparkConf)
    executorPodController.initialize(kubernetesClient)
  }

  test("Adding a pod and watching counter go up correctly") {
    val numAllocated = 5
    for ( _ <- 0 until numAllocated) {
      executorPodController.addPod(execPod)
    }
    verify(podOperations, times(numAllocated)).create(execPod)
    assert(executorPodController.commitAndGetTotalAllocated() == numAllocated)
    assert(executorPodController.commitAndGetTotalAllocated() == 0)
    executorPodController.addPod(execPod)
    assert(executorPodController.commitAndGetTotalAllocated() == 1)
  }

  test("Remove a single pod") {
    executorPodController.removePod(execPod)
    verify(execPodOperations).delete()
  }

  test("Remove a list of pods") {
    val execList = (1 to 3).map { num =>
      buildPod(s"$execExampleId-$num")
    }
    executorPodController.removePods(execList.asJava)
    verify(podOperations).delete(execList.asJava)
  }
}
