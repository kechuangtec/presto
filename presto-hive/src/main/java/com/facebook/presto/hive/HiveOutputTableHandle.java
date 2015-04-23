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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<Boolean> columnPartitioned;
    private final String tableOwner;
    private final String targetPath;
    private final String temporaryPath;
    private final ConnectorSession connectorSession;
    private final HiveStorageFormat hiveStorageFormat;

    @JsonCreator
    public HiveOutputTableHandle(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("columnPartitioned") List<Boolean> columnPartitioned,
            @JsonProperty("tableOwner") String tableOwner,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("temporaryPath") String temporaryPath,
            @JsonProperty("connectorSession") ConnectorSession connectorSession,
            @JsonProperty("hiveStorageFormat") HiveStorageFormat hiveStorageFormat)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.tableOwner = checkNotNull(tableOwner, "tableOwner is null");
        this.targetPath = checkNotNull(targetPath, "targetPath is null");
        this.temporaryPath = checkNotNull(temporaryPath, "temporaryPath is null");
        this.connectorSession = checkNotNull(connectorSession, "session is null");
        this.hiveStorageFormat = checkNotNull(hiveStorageFormat, "hiveStorageFormat is null");

        checkNotNull(columnNames, "columnNames is null");
        checkNotNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.columnPartitioned = columnPartitioned;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public List<String> getDataColumnNames()
    {
        if (!isOutputTablePartitioned()) {
            return columnNames;
        }

        List<String> dataColumnNames = new ArrayList<String>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (!columnPartitioned.get(i)) {
                dataColumnNames.add(columnNames.get(i));
            }
        }

        return dataColumnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public List<Type> getDataColumnTypes()
    {
        if (!isOutputTablePartitioned()) {
            return ImmutableList.copyOf(columnTypes);
        }

        List<Type> dataColumnTypes = new ArrayList<Type>();
        for (int i = 0; i < columnTypes.size(); i++) {
            if (!columnPartitioned.get(i)) {
                dataColumnTypes.add(columnTypes.get(i));
            }
        }

        return dataColumnTypes;
    }

    @JsonProperty
    public List<Boolean> getColumnPartitioned()
    {
        return columnPartitioned;
    }

    public boolean isOutputTablePartitioned()
    {
        return columnPartitioned != null && columnPartitioned.contains(true);
    }

    public List<String> getPartitionColumnNames()
    {
        if (!isOutputTablePartitioned()) {
            return null;
        }

        List<String> partitionColumnNames = new ArrayList<String>();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnPartitioned.get(i)) {
                partitionColumnNames.add(columnNames.get(i));
            }
        }

        return partitionColumnNames;
    }

    @JsonProperty
    public String getTableOwner()
    {
        return tableOwner;
    }

    @JsonProperty
    public String getTargetPath()
    {
        return targetPath;
    }

    @JsonProperty
    public String getTemporaryPath()
    {
        return temporaryPath;
    }

    @JsonProperty
    public ConnectorSession getConnectorSession()
    {
        return connectorSession;
    }

    @JsonProperty
    public HiveStorageFormat getHiveStorageFormat()
    {
        return hiveStorageFormat;
    }

    @Override
    public String toString()
    {
        return "hive:" + schemaName + "." + tableName;
    }

    public boolean hasTemporaryPath()
    {
        return !temporaryPath.equals(targetPath);
    }
}
