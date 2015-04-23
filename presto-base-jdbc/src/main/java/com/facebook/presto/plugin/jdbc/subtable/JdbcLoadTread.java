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

import static java.util.Locale.ENGLISH;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.facebook.presto.plugin.jdbc.util.JdbcUtil;
import com.mysql.jdbc.Driver;

public class JdbcLoadTread implements Runnable
{
    private static final Logger log = Logger.get(JdbcLoadTread.class);

    protected final String connectionUrl;
    protected final Properties connectionProperties;
    protected final String connectorId;
    protected final Duration jdbcReloadSubtableInterval;
    private long lastLoadSubTableTimeStamp = 0L;
    protected final Driver driver;

    private final ConcurrentMap<String, ArrayList<JdbcSubTableInfo>> allSubTables = new ConcurrentHashMap<>();

    public JdbcLoadTread(String connectionUrl,
            Properties connectionProperties,
            String connectorId,
            Duration jdbcReloadSubtableInterval) throws SQLException
    {
        this.connectionUrl = connectionUrl;
        this.connectionProperties = connectionProperties;
        this.connectorId = connectorId;
        this.jdbcReloadSubtableInterval = jdbcReloadSubtableInterval;
        this.driver = new Driver();
    }

    public void run()
    {
        while (true) {
            try {
                if (lastLoadSubTableTimeStamp == 0) {
                    loadSubTable();
                }
                else {
                    Thread.sleep(jdbcReloadSubtableInterval.toMillis());
                    long curTime = System.currentTimeMillis();
                    loadSubTable();
                    log.debug(connectorId + " load sub-table info spend time : " + (System.currentTimeMillis() - curTime) + " ms ");
                }
                lastLoadSubTableTimeStamp = System.currentTimeMillis();
            }
            catch (Exception e) {
                lastLoadSubTableTimeStamp = System.currentTimeMillis();
                log.error("Error reloading sub-table infomation", e);
            }
        }
    }

    public synchronized void loadSubTable()
    {
        Connection connection = null;
        ResultSet rs = null;
        Statement stat = null;
        String sql = "SELECT " + JdbcSubTableInfo.COLUMN_NAME +
            " FROM ROUTE_SCHEMA.TABLE_ROUTE" +
            " as a LEFT JOIN ROUTE_SCHEMA.DB_INFO as b on a.uid = b.uid" +
            " WHERE basecatalog = '" + connectorId + "'";
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            stat = connection.createStatement();
            rs = stat.executeQuery(sql);
            allSubTables.clear();
            //table columns : connectionurl,schemaname,tablename,basecatalog,baseschema,basetable,host,remotelyaccessible,
            // scannodenumber,autoincrementfield,username,password,fieldmaxvalue,fieldminvalue,pdboenable
            while (rs.next()) {
                JdbcSubTableInfo info = new JdbcSubTableInfo();
                info.setConnectionURL(rs.getString(1));
                info.setSchemaname(rs.getString(2).toLowerCase(ENGLISH));
                info.setTablename(rs.getString(3).toLowerCase(ENGLISH));
                info.setBasecatalog(rs.getString(4).toLowerCase(ENGLISH));
                info.setBaseschema(rs.getString(5).toLowerCase(ENGLISH));
                info.setBasetable(rs.getString(6).toLowerCase(ENGLISH));
                info.setHost(rs.getString(7));
                info.setRemotelyaccessible(rs.getString(8));
                info.setScannodenumber(rs.getInt(9));
                info.setAutoincrementfield(rs.getString(10));
                info.setUsername(rs.getString(11));
                info.setPassword(rs.getString(12));
                info.setFieldMaxValue(rs.getLong(13));
                info.setFieldMinValue(rs.getLong(14));
                info.setPdboEnable(rs.getString(15));
                String key = info.getBasecatalog() + JdbcUtil.SEPARATOR + info.getBaseschema() + JdbcUtil.SEPARATOR + info.getBasetable();
                //log.info("allSubTables key : " + key);
                ArrayList<JdbcSubTableInfo> arrayList = allSubTables.get(key);
                if (arrayList == null) {
                    arrayList = new ArrayList<JdbcSubTableInfo>();
                }
                arrayList.add(info);
                allSubTables.put(key, arrayList);
            }
        }
        catch (SQLException e) {
            log.error("SQL : " + sql + "Error reloading sub-table infomation : ", e.getMessage());
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
    }

    public List<PdboTable> getPDBOLogs(String connectorId, String schemaName, String tableName)
    {
        Connection connection = null;
        ResultSet rs = null;
        Statement stat = null;
        List<PdboTable> tables = new ArrayList<>();
        String sql = "SELECT A.CONNECTORID,A.SCHEMANAME,A.TABLENAME,A.ROWS,A.BEGININDEX,A.ENDINDEX,B.SCANNODENUMBER,"
                + " B.CONNECTIONURL,B.HOST,B.REMOTELYACCESSIBLE,B.AUTOINCREMENTFIELD,C.USERNAME,C.PASSWORD"
                + " FROM ROUTE_SCHEMA.PDBO_LOG A LEFT JOIN ROUTE_SCHEMA.TABLE_ROUTE B"
                + " ON A.CONNECTORID = B.BASECATALOG AND A.SCHEMANAME = B.BASESCHEMA AND A.TABLENAME = B.BASETABLE"
                + " LEFT JOIN ROUTE_SCHEMA.DB_INFO C ON B.UID = C.UID"
                + " WHERE A.CONNECTORID = '" + connectorId
                + "' AND A.SCHEMANAME = '" + schemaName
                + "' AND A.TABLENAME = '" + tableName
                + "' AND A.RECORDFLAG = 'finish' "
                + " AND B.PDBOENABLE = 'Y' "
                + " ORDER BY A.BEGININDEX";
        int scannodenumber = 0;
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            stat = connection.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                scannodenumber = rs.getInt(7);
                tables.add(new PdboTable().setConnectorId(rs.getString(1)).
                        setSchemaName(rs.getString(2)).
                        setTableName(rs.getString(3)).
                        setRows(rs.getLong(4)).
                        setBeginIndex(rs.getLong(5)).
                        setEndIndex(rs.getLong(6)).
                        setConnectionUrl(rs.getString(8)).
                        setHost(rs.getString(9)).
                        setRemotelyAccessible(rs.getString(10)).
                        setAutoIncrementField(rs.getString(11)).
                        setUsername(rs.getString(12)).
                        setPassword(rs.getString(13))
                );
            }
            if (scannodenumber != tables.size()) {
                tables.clear();
                loadSubTable();
            }
        }
        catch (SQLException e) {
            log.error("SQL : " + sql + "Error getPDBOLogs : ", e.getMessage());
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
        return tables;
    }

    public ConcurrentMap<String, ArrayList<JdbcSubTableInfo>> getAllSubTables()
    {
        return allSubTables;
    }
}
