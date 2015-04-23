/*
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
package com.facebook.presto.pdbo;

import io.airlift.log.Logger;

import java.sql.SQLException;
import java.util.Properties;

import javax.inject.Inject;

public class PdboManager
{
    private static final Logger log = Logger.get(PdboManager.class);

    private StepCalcManager stepCalcManager;
    @Inject
    public PdboManager(PdboConfig config)
    {
        boolean pdboExecuteEnable = config.getPdboExecuteEnable();
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", config.getPdboConnectionUser());
        connectionProperties.setProperty("password", config.getPdboConnectionPassword());
        if (pdboExecuteEnable) {
            try {
                stepCalcManager = new StepCalcManager(config.getPdboConnectionURL(),
                        connectionProperties, config.getPdboRefreshInterval(),
                        config.getPdboCleanHistoryInterval(), config.getPdboCalcThreads());
            }
            catch (SQLException e) {
                log.error("start StepCalcManager error", e.getMessage());
            }
            Thread stepCalcThread = new Thread(stepCalcManager);
            stepCalcThread.setName("step calc thread");
            stepCalcThread.setDaemon(true);
            stepCalcThread.start();
            stepCalcManager.start();
        }
    }
}
