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
package org.apache.spark.deploy.k8s.features.bindings

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

class PythonDriverFeatureStepSuite extends SparkFunSuite {


  test("Python Step modifies container correctly") {
    val expectedMainResource = "/main.py"
    val mainResource = "local:///main.py"
    val pyFiles = Seq("local:///example2.py", "local:///example3.py")
    val expectedPySparkFiles =
      "/example2.py,/example3.py"
    val baseDriverPod = SparkPod.initialPod()
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_PYSPARK_MAIN_APP_RESOURCE, mainResource)
      .set(KUBERNETES_PYSPARK_PY_FILES, pyFiles.mkString(","))
      .set("spark.files", "local:///example.py")
    val kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        Some(PythonMainAppResource("local:///main.py")),
        "test-app",
        "python-runner",
        Seq.empty[String]),
      "",
      "",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Seq.empty[String])

    val step = new PythonDriverFeatureStep(kubernetesConf)
    val driverPod = step.configurePod(baseDriverPod).pod
    val driverContainerwithPySpark = step.configurePod(baseDriverPod).container
    assert(driverContainerwithPySpark.getEnv.size === 2)
    val envs = driverContainerwithPySpark
      .getEnv
      .asScala
      .map(env => (env.getName, env.getValue))
      .toMap
    assert(envs(ENV_PYSPARK_PRIMARY) === expectedMainResource)
    assert(envs(ENV_PYSPARK_FILES) === expectedPySparkFiles)
  }
}
