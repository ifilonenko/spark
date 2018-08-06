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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import java.io.File

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, ContainerBuilder, KeyToPathBuilder, PodBuilder}

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkPod
import org.apache.spark.deploy.k8s.security.KubernetesHadoopDelegationTokenManager

private[spark] object HadoopBootstrapUtil {

   /**
    * Mounting the DT secret for both the Driver and the executors
    *
    * @param dtSecretName Name of the secret that stores the Delegation Token
    * @param dtSecretItemKey Name of the Item Key storing the Delegation Token
    * @param userName Name of the SparkUser to set SPARK_USER
    * @param fileLocation Location of the krb5 file
    * @param krb5ConfName Name of the ConfigMap for Krb5
    * @param pod Input pod to be appended to
    * @return a modified SparkPod
    */
  def bootstrapKerberosPod(
      dtSecretName: String,
      dtSecretItemKey: String,
      userName: String,
      fileLocation: String,
      krb5ConfName: String,
      pod: SparkPod) : SparkPod = {
      val krb5File = new File(fileLocation)
      val fileStringPath = krb5File.toPath.getFileName.toString
      val kerberizedPod = new PodBuilder(pod.pod)
        .editOrNewSpec()
          .addNewVolume()
            .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
            .withNewSecret()
              .withSecretName(dtSecretName)
              .endSecret()
            .endVolume()
          .addNewVolume()
            .withName(KRB_FILE_VOLUME)
              .withNewConfigMap()
                .withName(krb5ConfName)
                .withItems(new KeyToPathBuilder()
                  .withKey(fileStringPath)
                  .withPath(fileStringPath)
                  .build())
                .endConfigMap()
              .endVolume()
          .endSpec()
        .build()
      val kerberizedContainer = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
          .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
          .endVolumeMount()
        .addNewVolumeMount()
          .withName(KRB_FILE_VOLUME)
          .withMountPath(KRB_FILE_DIR_PATH)
          .endVolumeMount()
        .addNewEnv()
          .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
          .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$dtSecretItemKey")
          .endEnv()
        // TODO (ifilonenko): This has the correct user as ` userName` however
        // since the user to which the keytab has access to might not be on the k8s
        // nodes, this atm leaves us with the option to use `root`. Next step is to
        // support customization of the UNIX username.
        .addNewEnv()
          .withName(ENV_SPARK_USER)
          .withValue("root")
          .endEnv()
        .build()
    SparkPod(kerberizedPod, kerberizedContainer)
  }

   /**
    * setting ENV_SPARK_USER when HADOOP_FILES are detected
    *
    * @param sparkUserName Name of the SPARK_USER
    * @param pod Input pod to be appended to
    * @return a modified SparkPod
    */
  def bootstrapSparkUserPod(
     sparkUserName: String,
     pod: SparkPod) : SparkPod = {
     val envModifiedContainer = new ContainerBuilder(pod.container)
       .addNewEnv()
         .withName(ENV_SPARK_USER)
         .withValue(sparkUserName)
         .endEnv()
       .build()
    SparkPod(pod.pod, envModifiedContainer)
  }

   /**
    * bootstraping the container with ConfigMaps that store
    * Hadoop configuration files
    *
    * @param hadoopConfDir location of HADOOP_CONF_DIR
    * @param hadoopConfigMapName name of the configMap for HADOOP_CONF_DIR
    * @param kubeTokenManager KubernetesHadoopDelegationTokenManager
    * @param pod Input pod to be appended to
    * @return a modified SparkPod
    */
  def bootstrapHadoopConfDir(
    hadoopConfDir: String,
    hadoopConfigMapName: String,
    kubeTokenManager: KubernetesHadoopDelegationTokenManager,
    pod: SparkPod) : SparkPod = {
    val hadoopConfigFiles =
      kubeTokenManager.getHadoopConfFiles(hadoopConfDir)
    val keyPaths = hadoopConfigFiles.map { file =>
      val fileStringPath = file.toPath.getFileName.toString
      new KeyToPathBuilder()
        .withKey(fileStringPath)
        .withPath(fileStringPath)
        .build() }

    val hadoopSupportedPod = new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolume()
          .withName(HADOOP_FILE_VOLUME)
          .withNewConfigMap()
            .withName(hadoopConfigMapName)
            .withItems(keyPaths.asJava)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()

    val hadoopSupportedContainer = new ContainerBuilder(pod.container)
      .addNewVolumeMount()
        .withName(HADOOP_FILE_VOLUME)
        .withMountPath(HADOOP_CONF_DIR_PATH)
        .endVolumeMount()
      .addNewEnv()
        .withName(ENV_HADOOP_CONF_DIR)
        .withValue(HADOOP_CONF_DIR_PATH)
        .endEnv()
      .build()
    SparkPod(hadoopSupportedPod, hadoopSupportedContainer)
  }

  /**
    * bootstraping the container with ConfigMaps that store
    * Hadoop configuration files
    *
    * @param configMapName name of configMap for krb5
    * @param fileLocation location of krb5 file
    * @return a ConfigMap
    */
  def buildkrb5ConfigMap(
    configMapName: String,
    fileLocation: String) : ConfigMap = {
    val file = new File(fileLocation)
    new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName)
      .endMetadata()
      .addToData(
        Map(file.toPath.getFileName.toString -> Files.toString(file, Charsets.UTF_8)).asJava)
      .build()
  }
}
