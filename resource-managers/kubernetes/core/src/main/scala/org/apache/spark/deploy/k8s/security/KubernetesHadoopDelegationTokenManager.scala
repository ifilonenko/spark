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

package org.apache.spark.deploy.k8s.security

import java.io.File
import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config.KERBEROS_RELOGIN_PERIOD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.UpdateDelegationTokens
import org.apache.spark.util.ThreadUtils

/**
 * Adds Kubernetes-specific functionality to HadoopDelegationTokenManager.
 */
private[spark] class KubernetesHadoopDelegationTokenManager(
    _sparkConf: SparkConf,
    _hadoopConf: Configuration)
  extends HadoopDelegationTokenManager(_sparkConf, _hadoopConf) {

  def getCurrentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
  def isSecurityEnabled: Boolean = UserGroupInformation.isSecurityEnabled

  if (principal != null) {
    require(keytab != null, "Kerberos principal specified without a keytab.")
    require(new File(keytab).isFile, s"Cannot find keytab at $keytab.")
  }
  private val isTokenRenewalEnabled =
    _sparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_RENEWAL)

  if (isTokenRenewalEnabled) {
    require(_sparkConf.get(KUBERNETES_KERBEROS_DT_SECRET_NAME).isDefined,
      "Must specify the token secret which the driver must watch for updates")
  }

  private var watcher: Watcher[Secret] = _

  /**
   * As in HadoopDelegationTokenManager this starts the token renewer.
   * Upon start, if a principal has been configured, the renewer will:
   *
   * - log in the configured principal, and set up a task to keep that user's ticket renewed
   * - obtain delegation tokens from all available providers
   * - schedule a periodic task to update the tokens when needed.
   *
   * In the case that the principal is NOT configured, one may still service a long running
   * app by enabling the KERBEROS_SECRET_RENEWER config and relying on an external service
   * to populate a secret with valid Delegation Tokens that the application will then use.
   * This is possibly via the use of a Secret watcher which the driver will leverage to
   * detect updates that happen to the secret so that it may retrieve that secret's contents
   * and send it to all expiring executors
   *
   * @param driver If provided, the driver where to send the newly generated tokens.
   *               The same ref will also receive future token updates unless overridden later.
   * @return The newly logged in user, or null
   */
  override def start(driver: Option[RpcEndpointRef] = None): UserGroupInformation = {
    if (isTokenRenewalEnabled) {
      watcher = new Watcher[Secret] {
        override def onClose(cause: KubernetesClientException): Unit =
          logInfo("Ending the watch of DT Secret")

        override def eventReceived(action: Watcher.Action, resource: Secret): Unit = {
          action match {
            case Action.ADDED | Action.MODIFIED =>
              logInfo("Secret update")
              // TODO: Figure out what to do with secret here
          }
        }
      }
      null
    } else {
      super.start(driver)
    }
  }
}
