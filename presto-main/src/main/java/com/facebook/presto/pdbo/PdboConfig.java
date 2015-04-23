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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class PdboConfig
{
    public static final String DEFAULT_VALUE = "NA";
    private Duration pdboRefreshInterval = new Duration(3, TimeUnit.MINUTES);
    private Duration pdboCleanHistoryInterval = new Duration(6, TimeUnit.HOURS);
    private boolean pdboExecuteEnable = false;
    private String pdboConnectionURL = DEFAULT_VALUE;
    private String pdboConnectionUser = DEFAULT_VALUE;
    private String pdboConnectionPassword = DEFAULT_VALUE;
    private int pdboCalcThreads = 4;

    public Duration getPdboRefreshInterval()
    {
        return pdboRefreshInterval;
    }

    @Config("pdbo-refresh-interval")
    public PdboConfig setPdboRefreshInterval(Duration pdboRefreshInterval)
    {
        this.pdboRefreshInterval = pdboRefreshInterval;
        return this;
    }

    public Duration getPdboCleanHistoryInterval()
    {
        return pdboCleanHistoryInterval;
    }

    @Config("pdbo-clean-history-interval")
    public PdboConfig setPdboCleanHistoryInterval(Duration pdboCleanHistoryInterval)
    {
        this.pdboCleanHistoryInterval = pdboCleanHistoryInterval;
        return this;
    }

    public boolean getPdboExecuteEnable()
    {
        return pdboExecuteEnable;
    }

    @Config("pdbo-execute-enable")
    public PdboConfig setPdboExecuteEnable(boolean pdboExecuteEnable)
    {
        this.pdboExecuteEnable = pdboExecuteEnable;
        return this;
    }

    public String getPdboConnectionURL()
    {
        return pdboConnectionURL;
    }

    @Config("pdbo-connection-url")
    public PdboConfig setPdboConnectionURL(String pdboConnectionURL)
    {
        this.pdboConnectionURL = pdboConnectionURL;
        return this;
    }

    public String getPdboConnectionUser()
    {
        return pdboConnectionUser;
    }

    @Config("pdbo-connection-user")
    public PdboConfig setPdboConnectionUser(String pdboConnectionUser)
    {
        this.pdboConnectionUser = pdboConnectionUser;
        return this;
    }

    public String getPdboConnectionPassword()
    {
        return pdboConnectionPassword;
    }

    @Config("pdbo-connection-password")
    public PdboConfig setPdboConnectionPassword(String pdboConnectionPassword)
    {
        this.pdboConnectionPassword = pdboConnectionPassword;
        return this;
    }

    public int getPdboCalcThreads()
    {
        return pdboCalcThreads;
    }

    @Config("pdbo-calc-threads")
    public PdboConfig setPdboCalcThreads(int pdboCalcThreads)
    {
        this.pdboCalcThreads = pdboCalcThreads;
        return this;
    }
}
