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
package org.apache.spark.deploy.k8s

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.client.Config._
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import okhttp3.Dispatcher

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.ThreadUtils

/**
 * Spark-opinionated builder for Kubernetes clients. It uses a prefix plus common suffixes to
 * parse configuration keys, similar to the manner in which Spark's SecurityManager parses SSL
 * options for different components.
 */
private[spark] object SparkKubernetesClientFactory {

  def getDriverKubernetesClient(conf: SparkConf, masterURL: String): KubernetesClient = {
    val wasSparkSubmittedInClusterMode = conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK)
    val (authConfPrefix,
    apiServerUri,
    defaultServiceAccountToken,
    defaultServiceAccountCaCrt) = if (wasSparkSubmittedInClusterMode) {
      require(conf.get(KUBERNETES_DRIVER_POD_NAME).isDefined,
        "If the application is deployed using spark-submit in cluster mode, the driver pod name " +
          "must be provided.")
      (KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        KUBERNETES_MASTER_INTERNAL_URL,
        Some(new File(KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
        Some(new File(KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))
    } else {
      (KUBERNETES_AUTH_CLIENT_MODE_PREFIX,
        KubernetesUtils.parseMasterUrl(masterURL),
        None,
        None)
    }

    val kubernetesClient = createKubernetesClient(
      apiServerUri,
      Some(conf.get(KUBERNETES_NAMESPACE)),
      authConfPrefix,
      conf,
      defaultServiceAccountToken,
      defaultServiceAccountCaCrt)
    kubernetesClient
  }

  def createKubernetesClient(
      master: String,
      namespace: Option[String],
      kubernetesAuthConfPrefix: String,
      sparkConf: SparkConf,
      defaultServiceAccountToken: Option[File],
      defaultServiceAccountCaCert: Option[File]): KubernetesClient = {

    // TODO [SPARK-25887] Support configurable context

    val oauthTokenFileConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_FILE_CONF_SUFFIX"
    val oauthTokenConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_CONF_SUFFIX"
    val oauthTokenFile = sparkConf.getOption(oauthTokenFileConf)
      .map(new File(_))
      .orElse(defaultServiceAccountToken)
    val oauthTokenValue = sparkConf.getOption(oauthTokenConf)
    KubernetesUtils.requireNandDefined(
      oauthTokenFile,
      oauthTokenValue,
      s"Cannot specify OAuth token through both a file $oauthTokenFileConf and a " +
        s"value $oauthTokenConf.")

    val caCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CA_CERT_FILE_CONF_SUFFIX")
      .orElse(defaultServiceAccountCaCert.map(_.getAbsolutePath))
    val clientKeyFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_KEY_FILE_CONF_SUFFIX")
    val clientCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_CERT_FILE_CONF_SUFFIX")
    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))

    // TODO [SPARK-25887] Create builder in a way that respects configurable context
    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withWebsocketPingInterval(0)
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }.withOption(oauthTokenFile) {
        (file, configBuilder) =>
            configBuilder.withOauthToken(Files.toString(file, Charsets.UTF_8))
      }.withOption(caCertFile) {
        (file, configBuilder) => configBuilder.withCaCertFile(file)
      }.withOption(clientKeyFile) {
        (file, configBuilder) => configBuilder.withClientKeyFile(file)
      }.withOption(clientCertFile) {
        (file, configBuilder) => configBuilder.withClientCertFile(file)
      }.withOption(namespace) {
        (ns, configBuilder) => configBuilder.withNamespace(ns)
      }.build()
    val baseHttpClient = HttpClientUtils.createHttpClient(config)
    val httpClientWithCustomDispatcher = baseHttpClient.newBuilder()
      .dispatcher(dispatcher)
      .build()
    new DefaultKubernetesClient(httpClientWithCustomDispatcher, config)
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T]
        (option: Option[T])
        (configurator: ((T, ConfigBuilder) => ConfigBuilder)): ConfigBuilder = {
      option.map { opt =>
        configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }
}
