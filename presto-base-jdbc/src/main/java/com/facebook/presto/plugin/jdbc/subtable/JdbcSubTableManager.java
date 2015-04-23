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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Locale.ENGLISH;
import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.facebook.presto.plugin.jdbc.JdbcPartition;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.util.JdbcUtil;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;

public class JdbcSubTableManager
{
    private static final Logger log = Logger.get(JdbcSubTableManager.class);

    protected final String connectorId;
    protected final String identifierQuote;
    protected final Driver baseDriver;
    protected Driver mySqlDriver;
    protected final String defaultConnectionUrl;
    protected final Properties defaultConnectionProperties;
    protected final String jdbcSubTableConnectionUrl;
    protected final Properties jdbcSubTableConnectionProperties;

    private JdbcLoadTread loadTread;

    public JdbcSubTableManager(String connectorId,
            String identifierQuote,
            Driver driver,
            String defaultConnectionUrl,
            Properties defaultConnectionProperties,
            JdbcSubTableConfig config)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.identifierQuote = checkNotNull(identifierQuote, "identifierQuote is null");
        this.baseDriver = checkNotNull(driver, "driver is null");
        this.defaultConnectionUrl = defaultConnectionUrl;
        this.defaultConnectionProperties = defaultConnectionProperties;

        checkNotNull(config, "config is null");
        jdbcSubTableConnectionUrl = config.getJdbcSubTableConnectionURL();
        jdbcSubTableConnectionProperties = new Properties();
        jdbcSubTableConnectionProperties.setProperty("user", config.getJdbcSubTableConnectionUser());
        jdbcSubTableConnectionProperties.setProperty("password", config.getJdbcSubTableConnectionPassword());

        // only the allocator should start the reload thread
        if (config.getJdbcSubTableAllocator() && config.getJdbcSubTableEnable()) {
            try {
                loadTread = new JdbcLoadTread(config.getJdbcSubTableConnectionURL(), jdbcSubTableConnectionProperties,
                        connectorId, config.getJdbcReloadSubtableInterval());
            }
            catch (SQLException e) {
                log.error("Init JdbcLoadTread error", e);
            }
            Thread loadTableThread = new Thread(loadTread);
            loadTableThread.setName("LoadTableThread");
            loadTableThread.setDaemon(true);
            loadTableThread.start();
        }
        try {
            this.mySqlDriver = new com.mysql.jdbc.Driver();
        }
        catch (SQLException e) {
            log.error("Init MySQL Driver error", e);
        }
    }

    /**
     * Get table splits
     * @param jdbcPartition
     * @return
     */
    public ConnectorSplitSource getTableSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        List<JdbcSplit> jdbcSplitsList = new ArrayList<JdbcSplit>();
        String schemaName = getSchemaName(jdbcTableHandle);
        String key = connectorId + JdbcUtil.SEPARATOR + schemaName + JdbcUtil.SEPARATOR + jdbcTableHandle.getTableName().toLowerCase(ENGLISH);
        ArrayList<JdbcSubTableInfo> subTableList = loadTread.getAllSubTables().get(key);
        log.debug("key=" + key + ",subTableList size = " + (subTableList == null ? 0 : subTableList.size()));
        if (JdbcUtil.checkListNullOrEmpty(subTableList)) {
            if (subTableList == null) {
                subTableList = new ArrayList<JdbcSubTableInfo>();
            }
            JdbcSubTableInfo config = new JdbcSubTableInfo();
            config.setConnectionURL(defaultConnectionUrl);
            config.setCatalogname(jdbcTableHandle.getCatalogName());
            config.setSchemaname(jdbcTableHandle.getSchemaName());
            config.setTablename(jdbcTableHandle.getTableName());
            config.setRemotelyaccessible("Y");
            config.setBasetable(jdbcTableHandle.getTableName());
            subTableList.add(config);
        }
        long timeStamp = System.nanoTime();
        if (subTableList.get(0).isPdboEnable()) {
            jdbcSplitsList = getTableSplitsFromPdboLog(connectorId, schemaName, jdbcTableHandle.getTableName().toLowerCase(ENGLISH), jdbcPartition, timeStamp);
        }
        if (JdbcUtil.checkListNullOrEmpty(jdbcSplitsList)) {
            for (JdbcSubTableInfo config : subTableList) {
                constructJdbcSplits(jdbcPartition, jdbcSplitsList, config, timeStamp);
            }
        }
        return new FixedSplitSource(connectorId, jdbcSplitsList);
    }

    private String getSchemaName(JdbcTableHandle jdbcTableHandle)
    {
        String schemaName = "";
        if (defaultConnectionUrl.indexOf("mysql") != -1) {
            schemaName = jdbcTableHandle.getCatalogName().toLowerCase(ENGLISH);
        }
        else {
            schemaName = jdbcTableHandle.getSchemaName().toLowerCase(ENGLISH);
        }
        return schemaName;
    }

    private void constructJdbcSplits(JdbcPartition jdbcPartition,
            List<JdbcSplit> splits, JdbcSubTableInfo config, long timeStamp)
    {
        List<HostAddress> addresses = getSplitHost(config.getHost());
        Properties connectionProperties = resetConnectionProperties(config.getUsername(), config.getPassword());
        int scanNodes = config.getScannodenumber() <= 0 ? 1 : config.getScannodenumber();
        if (scanNodes == 1) {
            addJdbcSplit(jdbcPartition, splits, config.getCatalogname(), config.getSchemaname(), config.getTablename(),
                    config.getConnectionURL(), config.getBasetable(), config.getRemotelyaccessible(), config.getAutoincrementfield(),
                    addresses, new String[]{"", "", ""}, connectionProperties, timeStamp, scanNodes, false);
        }
        else {
            splitTable(jdbcPartition, splits, config, addresses,
                    connectionProperties, scanNodes, timeStamp);
        }
    }

    /**
     * Splitting table by field or limit
     */
    private void splitTable(JdbcPartition jdbcPartition,
            List<JdbcSplit> splits, JdbcSubTableInfo config,
            List<HostAddress> addresses, Properties connectionProperties,
            int scanNodes, long timeStamp)
    {
        long tableTotalRecords = 0L;
        Long[] autoIncrementFieldMinAndMaxValue = new Long[2];
        boolean isSplitByField = isNullOrEmpty(config.getAutoincrementfield());
        if (!isSplitByField) {
            autoIncrementFieldMinAndMaxValue = getSplitFieldMinAndMaxValue(config, connectionProperties);
            tableTotalRecords = autoIncrementFieldMinAndMaxValue[0] - autoIncrementFieldMinAndMaxValue[1];
        }
        else {
            tableTotalRecords = getTableTotalRecords(config, connectionProperties);
        }
        long targetChunkSize = (long) Math.ceil(tableTotalRecords * 1.0 / scanNodes);
        long chunkOffset = 0L;
        long autoIncrementOffset = 0L;
        while (chunkOffset < tableTotalRecords) {
            long chunkLength = Math.min(targetChunkSize, tableTotalRecords - chunkOffset);
            if (!isSplitByField && chunkOffset == 0) {
                    autoIncrementOffset = autoIncrementFieldMinAndMaxValue[1] - 1;
            }
            String[] splitInfo = getSplitInfo(isSplitByField, chunkOffset, autoIncrementOffset,
                    autoIncrementFieldMinAndMaxValue, chunkLength, tableTotalRecords, config.getAutoincrementfield());
            addJdbcSplit(jdbcPartition, splits, config.getCatalogname(), config.getSchemaname(), config.getTablename(),
                    config.getConnectionURL(), config.getBasetable(), config.getRemotelyaccessible(), config.getAutoincrementfield(),
                    addresses, splitInfo, connectionProperties, timeStamp, scanNodes, config.isPdboEnable());
            chunkOffset += chunkLength;
            autoIncrementOffset += chunkLength;
        }
        fillLastRecord(jdbcPartition, config.getSchemaname(), config.getTablename(), splits, scanNodes,
                timeStamp, connectionProperties, config.getConnectionURL(), config.getHost(),
                config.getRemotelyaccessible(), config.getAutoincrementfield(),
                isSplitByField ? tableTotalRecords : config.getFieldMaxValue(), isSplitByField, false);
    }

    /**
     * If the table split by field,the filter conditions will follow like this :
     *      field > offset and field <= offset + chunkLength.
     * The limit condition only support MySQL connector
     * @return splitInfo[0] : splitPart; splitInfo[1] : beginIndex; splitInfo[2] : endIndex;
     */
    private String[] getSplitInfo(boolean isSplitByField, long chunkOffset, long autoIncrementOffset,
            Long[] autoIncrementFieldMinAndMaxValue, long chunkLength, long tableTotalRecords, String splitField)
    {
        String[] splitInfo = new String[3];
        String splitPart = "";
        if (!isSplitByField) {
            splitInfo[1] = String.valueOf(autoIncrementOffset);
            splitPart = splitField + " > " + autoIncrementOffset + " and " + splitField + " <= ";
            if ((chunkOffset + chunkLength) == tableTotalRecords) {
                splitPart += autoIncrementFieldMinAndMaxValue[0];
                splitInfo[2] = String.valueOf(autoIncrementFieldMinAndMaxValue[0]);
            }
            else {
                splitPart += (autoIncrementOffset + chunkLength);
                splitInfo[2] = String.valueOf(autoIncrementOffset + chunkLength);
            }
        }
        else {
            splitPart = " LIMIT " + chunkOffset + "," + chunkLength;
        }
        splitInfo[0] = splitPart;
        return splitInfo;
    }

    private void addJdbcSplit(JdbcPartition jdbcPartition,
            List<JdbcSplit> builder, String catalogName, String schemaName,
            String tableName, String connectionUrl, String baseTable,
            boolean remotelyAccessible, String splitField,
            List<HostAddress> addresses, String[] splitInfo,
            Properties connectionProperties, long timeStamp, int scanNodes, boolean pdboEnable)
    {
        builder.add(new JdbcSplit(connectorId, catalogName, schemaName, tableName,
                connectionUrl, fromProperties(connectionProperties), jdbcPartition.getTupleDomain(),
                splitInfo[0], addresses, remotelyAccessible, baseTable,
                splitField, splitInfo[1], splitInfo[2], timeStamp, scanNodes, pdboEnable));
    }

    protected long getTableTotalRecords(JdbcSubTableInfo config, Properties connectionProperties)
    {
        String sql = "";
        if (config.getConnectionURL().indexOf("mysql") != -1) {
            sql = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES " +
                    " WHERE TABLE_SCHEMA = '" + config.getSchemaname() + "' AND TABLE_NAME = '" + config.getTablename() + "'";
        }
        else if (config.getConnectionURL().indexOf("sqlserver") != -1) {
            sql = "SELECT i.rows FROM sys.tables t JOIN sys.sysindexes i " +
                " ON t.object_id=i.id AND i.indid IN (0,1) WHERE t.name = '" + config.getTablename() + "'";
        }
        else {
            sql = "SELECT COUNT(1) FROM " + JdbcUtil.getTableName(identifierQuote, config.getCatalogname(), config.getSchemaname(), config.getTablename());
        }
        return getTableRows(config.getConnectionURL(), connectionProperties, sql);
    }

    private long getTableRows(String connectionUrl,
            Properties connectionProperties, String sql)
    {
        Connection connection = null;
        Statement stat = null;
        ResultSet rs = null;
        long recordNum = 0L;
        try {
            connection = baseDriver.connect(connectionUrl, connectionProperties);
            stat = connection.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                recordNum = rs.getLong(1);
            }
        }
        catch (SQLException e) {
            log.error("SQL : " + sql + ",getTableRows error : " + e.getMessage());
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
        return recordNum;
    }

    protected Long[] getSplitFieldMinAndMaxValue(JdbcSubTableInfo conf, Properties connectionProperties)
    {
        Long[] value = new Long[2];
        if (conf.getFieldMaxValue() > 0) {
            value[0] = conf.getFieldMaxValue();
            value[1] = conf.getFieldMinValue();
            return value;
        }
        String sql = "SELECT MAX(" + conf.getAutoincrementfield() + "),MIN(" + conf.getAutoincrementfield() + ") FROM "
                    + JdbcUtil.getTableName(identifierQuote, conf.getCatalogname(), conf.getSchemaname(), conf.getTablename());
        Connection connection = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            connection = baseDriver.connect(conf.getConnectionURL(), connectionProperties);
            stat = connection.createStatement();
            rs = stat.executeQuery(sql.toString());
            while (rs.next()) {
                value[0] = rs.getLong(1);
                value[1] = rs.getLong(2);
            }
        }
        catch (SQLException e) {
            log.error("SQL : " + sql + ",getSplitFieldMinAndMaxValue error : " + e.getMessage());
            return null;
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
        return value;
    }

    public List<JdbcSplit> getTableSplitsFromPdboLog(String catalogName, String schemaName, String tableName,
            JdbcPartition jdbcPartition, long timeStamp)
    {
        List<JdbcSplit> splits = new ArrayList<>();
        List<PdboTable> pdboLogs = loadTread.getPDBOLogs(catalogName, schemaName, tableName);
        if (JdbcUtil.checkListNullOrEmpty(pdboLogs)) {
            return splits;
        }
        int scanNodes = pdboLogs.size();
        for (PdboTable table : pdboLogs) {
            List<HostAddress> addresses = getSplitHost(table.getHost());
            Properties connectionProperties = resetConnectionProperties(table.getUsername(), table.getPassword());
            String splitPart = table.getAutoIncrementField() + " > " + table.getBeginIndex() + " and "
                    + table.getAutoIncrementField() + " <= " + table.getEndIndex();
            addJdbcSplit(jdbcPartition, splits, null, schemaName, tableName, table.getConnectionUrl(),
                    tableName, table.getRemotelyAccessible(), table.getAutoIncrementField(), addresses,
                    new String[]{splitPart, String.valueOf(table.getBeginIndex()), String.valueOf(table.getEndIndex())},
                    connectionProperties, timeStamp, scanNodes, true);
        }
        PdboTable lastRecord = pdboLogs.get(scanNodes - 1);
        fillLastRecord(jdbcPartition, schemaName, tableName, splits, scanNodes,
                timeStamp, resetConnectionProperties(lastRecord.getUsername(), lastRecord.getPassword()),
                lastRecord.getConnectionUrl(), lastRecord.getHost(), lastRecord.getRemotelyAccessible(),
                lastRecord.getAutoIncrementField(), lastRecord.getEndIndex(), false, true);
        return splits;
    }

    private void fillLastRecord(JdbcPartition jdbcPartition, String schemaName, String tableName,
            List<JdbcSplit> splits, int scanNodes, long timeStamp, Properties connectionProperties,
            String connectionURL, String host, boolean remotelyAccessible, String splitField,
            long endIndex, boolean limitFlag, boolean pdboEnable)
    {
        String splitPart = "";
        if (limitFlag) {
            splitPart = " LIMIT " + endIndex + ",1000000000";
        }
        else {
            splitPart = splitField + " > " + endIndex;
        }
        addJdbcSplit(jdbcPartition, splits, null, schemaName, tableName, connectionURL,
                tableName, remotelyAccessible, null, getSplitHost(host),
                new String[]{splitPart, "", ""}, connectionProperties, timeStamp, scanNodes, pdboEnable);
    }

    private Properties resetConnectionProperties(String username, String password)
    {
        Properties connectionProperties = (Properties) defaultConnectionProperties.clone();
        if (!isNullOrEmpty(username) && !isNullOrEmpty(password)) {
            connectionProperties.setProperty("user", username);
            connectionProperties.setProperty("password", password);
        }
        return connectionProperties;
    }

    private List<HostAddress> getSplitHost(String host)
    {
        return isNullOrEmpty(host) ? ImmutableList.of() : ImmutableList.of(HostAddress.fromString(host));
    }

    public void commitPdboLogs(JdbcSplit split, long rowCount)
    {
        Connection connection = null;
        Statement stat = null;
        ResultSet rs = null;
        StringBuilder insertSql = new StringBuilder().append("INSERT INTO ROUTE_SCHEMA.PDBO_LOG "
                + " (CONNECTORID,SCHEMANAME,TABLENAME,ROWS,BEGININDEX,ENDINDEX,RECORDFLAG,SCANNODES,TIMESTAMP) VALUES ")
            .append("('" + connectorId + "',")
            .append("'" + split.getSchemaName() + "',")
            .append("'" + split.getTableName() + "',")
            .append(rowCount + ",")
            .append(split.getBeginIndex() + ",")
            .append(split.getEndIndex() + ",")
            .append("'new',")
            .append(split.getScanNodes() + ",")
            .append(split.getTimeStamp() + ")");
        String updateSql = "UPDATE ROUTE_SCHEMA.PDBO_LOG SET RECORDFLAG='runhistory' "
                + "WHERE RECORDFLAG='new' AND CONNECTORID='" + connectorId
                + "' AND SCHEMANAME='" + split.getSchemaName() + "' AND TABLENAME='"
                + split.getTableName() + "'" + " AND timestamp < " + split.getTimeStamp();
        try {
            connection = mySqlDriver.connect(jdbcSubTableConnectionUrl, jdbcSubTableConnectionProperties);
            stat = connection.createStatement();
            stat.execute(updateSql);
            stat.execute(insertSql.toString());
        }
        catch (SQLException e) {
            log.error("insert sql : " + insertSql + ",update sql : " + updateSql + "commitPdboLogs error : " + e.getMessage());
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
    }
}
