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
package com.facebook.presto.plugin.sqlserver;

import java.sql.SQLException;

import javax.inject.Inject;

import net.sourceforge.jtds.jdbc.Driver;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.cache.JdbcCacheConfig;
import com.facebook.presto.plugin.jdbc.subtable.JdbcSubTableConfig;

public class SqlServerClient
        extends BaseJdbcClient
{
    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
            JdbcSubTableConfig subTableConfig, JdbcCacheConfig cacheConfig)
            throws SQLException
    {
        super(connectorId, config, "\"", new Driver(), subTableConfig, cacheConfig);
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
    }
}
