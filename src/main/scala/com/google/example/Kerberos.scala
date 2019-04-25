/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.example

import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.ImmutableMap
import com.sun.security.auth.module.Krb5LoginModule
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

object Kerberos {
  def configureJaas(configName: String, keyTab: String, principal: String): Unit = {
    Configuration.setConfiguration(
      new Configuration() {
        private val ConfigName = configName
        @Override
        def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
          checkArgument(ConfigName.equals(name))
          Array(
            new AppConfigurationEntry(
              classOf[Krb5LoginModule].getCanonicalName,
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              ImmutableMap.builder()
                .put("principal", principal)
                .put("keyTab", keyTab)
                .put("doNotPrompt", "true")
                .put("useKeyTab", "true")
                .put("debug", "false")
                .put("storeKey", "true")
                .put("useTicketCache", "false")
                .put("refreshKrb5Config", "true")
                .build()
            )
          )
        }
      }
    )
  }
}
