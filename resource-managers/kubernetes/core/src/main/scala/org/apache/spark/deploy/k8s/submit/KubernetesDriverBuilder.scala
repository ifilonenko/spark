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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf}
import org.apache.spark.deploy.k8s.features.{BasicDriverFeatureStep, DriverKubernetesCredentialsFeatureStep, DriverServiceFeatureStep, MountSecretsFeatureStep}
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep
import org.apache.spark.deploy.k8s.features.bindings.PythonDriverFeatureStep

private[spark] class KubernetesDriverBuilder(
    provideBasicStep: (KubernetesConf[KubernetesDriverSpecificConf]) => BasicDriverFeatureStep =
      new BasicDriverFeatureStep(_),
    provideCredentialsStep: (KubernetesConf[KubernetesDriverSpecificConf])
      => DriverKubernetesCredentialsFeatureStep =
      new DriverKubernetesCredentialsFeatureStep(_),
    provideServiceStep: (KubernetesConf[KubernetesDriverSpecificConf]) => DriverServiceFeatureStep =
      new DriverServiceFeatureStep(_),
    provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => MountSecretsFeatureStep) =
      new MountSecretsFeatureStep(_),
    providePythonStep: (
      KubernetesConf[_ <: KubernetesRoleSpecificConf]
      => PythonDriverFeatureStep) =
      new PythonDriverFeatureStep(_)) {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf]): KubernetesDriverSpec = {
    val baseFeatures = Seq(
      provideBasicStep(kubernetesConf),
      provideCredentialsStep(kubernetesConf),
      provideServiceStep(kubernetesConf))
    val maybeRoleSecretNamesStep = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Some(provideSecretsStep(kubernetesConf)) } else None
    val maybeNonJVMBindings = kubernetesConf.roleSpecificConf.mainAppResource.getOrElse(None)
      match {
        case PythonMainAppResource(_) =>
          Some(providePythonStep(kubernetesConf))
        case _ => None
    }
    val allFeatures: Seq[KubernetesFeatureConfigStep] =
      baseFeatures ++
      maybeRoleSecretNamesStep.toSeq ++
      maybeNonJVMBindings.toSeq
    var spec = KubernetesDriverSpec.initialSpec(kubernetesConf.sparkConf.getAll.toMap)
    for (feature <- allFeatures) {
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedResources = feature.getAdditionalKubernetesResources()
      spec = KubernetesDriverSpec(
        configuredPod,
        spec.driverKubernetesResources ++ addedResources,
        spec.systemProperties ++ addedSystemProperties)
    }
    spec
  }
}
