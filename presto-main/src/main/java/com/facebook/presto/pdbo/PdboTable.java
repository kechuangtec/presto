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

import com.google.common.primitives.Longs;

public class PdboTable implements Comparable<PdboTable>
{
    private String connectorId;
    private String schemaName;
    private String tableName;
    private Long rows;
    private Long beginIndex;
    private Long endIndex;
    private String recordFlag;
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

    public Integer getScanNodes()
    {
        return scanNodes;
    }

    public PdboTable setScanNodes(Integer scanNodes)
    {
        this.scanNodes = scanNodes;
        return this;
    }

    @Override
    public int compareTo(PdboTable o)
    {
        return Longs.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public String toString()
    {
        return getConnectorId() + "-"
                + getSchemaName() + "-"
                + getTableName() + "-"
                + getBeginIndex() + "-"
                + getEndIndex() + "-";
    }
}
