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
package com.facebook.presto.plugin.jdbc.subtable;

public class PdboTable
{
    private String connectorId;
    private String schemaName;
    private String tableName;
    private Long rows;
    private Long beginIndex;
    private Long endIndex;
    private String recordFlag;
    private String connectionUrl;
    private String host;
    private String remotelyAccessible;
    private String autoIncrementField;
    private String username;
    private String password;
    private Long timeStamp;
    private Integer scanNodes;

    public String getConnectorId()
    {
        return connectorId;
    }

    public PdboTable setConnectorId(String connectorId)
    {
        this.connectorId = connectorId;
        return this;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public PdboTable setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public PdboTable setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public Long getRows()
    {
        return rows;
    }

    public PdboTable setRows(Long rows)
    {
        this.rows = rows;
        return this;
    }

    public Long getBeginIndex()
    {
        return beginIndex;
    }

    public PdboTable setBeginIndex(Long beginIndex)
    {
        this.beginIndex = beginIndex;
        return this;
    }

    public Long getEndIndex()
    {
        return endIndex;
    }

    public PdboTable setEndIndex(Long endIndex)
    {
        this.endIndex = endIndex;
        return this;
    }

    public String getRecordFlag()
    {
        return recordFlag;
    }

    public PdboTable setRecordFlag(String recordFlag)
    {
        this.recordFlag = recordFlag;
        return this;
    }

    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    public PdboTable setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getHost()
    {
        return host;
    }

    public PdboTable setHost(String host)
    {
        this.host = host;
        return this;
    }

    public boolean getRemotelyAccessible()
    {
        if (remotelyAccessible == null || "".equals(remotelyAccessible)) {
            return true;
        }
        else if ("Y".equals(remotelyAccessible)
                || "y".equals(remotelyAccessible)) {
            return true;
        }
        else {
            return false;
        }
    }

    public PdboTable setRemotelyAccessible(String remotelyAccessible)
    {
        this.remotelyAccessible = remotelyAccessible;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    public PdboTable setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    public PdboTable setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getAutoIncrementField()
    {
        return autoIncrementField;
    }

    public PdboTable setAutoIncrementField(String autoIncrementField)
    {
        this.autoIncrementField = autoIncrementField;
        return this;
    }

    public Long getTimeStamp()
    {
        return timeStamp;
    }

    public PdboTable setTimeStamp(Long timeStamp)
    {
        this.timeStamp = timeStamp;
        return this;
    }

    public Integer getScanNodes()
    {
        return scanNodes;
    }

    public PdboTable setScanNodes(Integer scanNodes)
    {
        this.scanNodes = scanNodes;
        return this;
    }
}
