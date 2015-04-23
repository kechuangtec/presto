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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.cache.JdbcCacheSplit;
import com.facebook.presto.plugin.jdbc.cache.JdbcJavaBean;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import org.joda.time.chrono.ISOChronology;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.joda.time.DateTimeZone.UTC;

public class JdbcRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(JdbcRecordCursor.class);

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);

    private final List<JdbcColumnHandle> columnHandles;

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private boolean closed;

    private List<JdbcJavaBean> tableDataSet;
    private boolean isCacheTable = false;
    private AtomicLong rowRecord = new AtomicLong(0);
    private BaseJdbcClient client;
    private JdbcSplit split;

    public JdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.client = (BaseJdbcClient) jdbcClient;
        this.split = split;
        isCacheTable = client.isCacheTable(split.getBaseTableName());
        this.columnHandles = ImmutableList.copyOf(checkNotNull(columnHandles, "columnHandles is null"));
        if (isCacheTable) {
            JdbcCacheSplit key = new JdbcCacheSplit(split.getConnectorId(), split.getCatalogName(),
                    split.getSchemaName(), split.getTableName(), split.getConnectionUrl(), split.getBaseTableName());
            tableDataSet = client.getTableDataSet(key);
        }
        else {
            String sql = jdbcClient.buildSql(split, columnHandles);
            try {
                connection = jdbcClient.getConnection(split);
                statement = connection.createStatement();
                statement.setFetchSize(1000);

                String whereCondition = split.getSplitPart();
                if (!isNullOrEmpty(whereCondition)) {
                    if (whereCondition.indexOf("LIMIT") != -1) {
                        sql += split.getSplitPart();
                    }
                    else {
                        if (sql.indexOf("WHERE") != -1) {
                            sql += " AND " + split.getSplitPart();
                        }
                        else {
                            sql += " WHERE " + split.getSplitPart();
                        }
                    }
                }
                long startTime = System.currentTimeMillis();
                log.info("JdbcRecordCursor Executing: %s ", sql);
                resultSet = statement.executeQuery(sql);
                log.debug("The connection url: %s ,JdbcRecordCursor Executing: %s ,spend time : %s", split.getConnectionUrl(), sql, (System.currentTimeMillis() - startTime));
            }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }
        boolean result;
        if (isCacheTable) {
            long andIncrement = rowRecord.incrementAndGet();
            result = andIncrement <= tableDataSet.size();
        }
        else {
            try {
                result = resultSet.next();
                if (result) {
                    rowRecord.getAndIncrement();
                }
            }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }
        if (!result) {
            close();
        }
        return result;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        if (isCacheTable) {
            return (boolean) getFieldValue(field);
        }
        else {
            try {
                return resultSet.getBoolean(field + 1);
            }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (type.equals(BigintType.BIGINT)) {
                if (isCacheTable) {
                    return (long) getFieldValue(field);
                }
                else {
                    return resultSet.getLong(field + 1);
                }
            }
            if (type.equals(DateType.DATE)) {
                Date date = null;
                if (isCacheTable) {
                    date = (Date) getFieldValue(field);
                }
                else {
                    date = resultSet.getDate(field + 1);
                }
                // JDBC returns a date using a timestamp at midnight in the JVM timezone
                long localMillis = date.getTime();
                // Convert it to a midnight in UTC
                long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
                // convert to days
                return TimeUnit.MILLISECONDS.toDays(utcMillis);
            }
            if (type.equals(TimeType.TIME)) {
                Time time = null;
                if (isCacheTable) {
                    time = (Time) getFieldValue(field);
                }
                else {
                    time = resultSet.getTime(field + 1);
                }
                return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
            }
            if (type.equals(TimestampType.TIMESTAMP)) {
                Timestamp timestamp = null;
                if (isCacheTable) {
                    timestamp = (Timestamp) getFieldValue(field);
                }
                else {
                    timestamp = resultSet.getTimestamp(field + 1);
                }
                return timestamp.getTime();
            }
            throw new PrestoException(INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        if (isCacheTable) {
            return (double) getFieldValue(field);
        }
        else {
            try {
                    return resultSet.getDouble(field + 1);
                }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            Type type = getType(field);
            if (type.equals(VarcharType.VARCHAR)) {
                String str = null;
                if (isCacheTable) {
                    str = (String) getFieldValue(field);
                }
                else {
                    str = resultSet.getString(field + 1);
                }
                return utf8Slice(str);
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                byte[] bytes = null;
                if (isCacheTable) {
                    bytes = (byte[]) getFieldValue(field);
                }
                else {
                    bytes = resultSet.getBytes(field + 1);
                }
                return wrappedBuffer(bytes);
            }
            throw new PrestoException(INTERNAL_ERROR, "Unhandled type for slice: " + type.getTypeSignature());
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");

        try {
            if (isCacheTable) {
                Object feildValue = getFieldValue(field);
                return feildValue == null;
            }
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(field + 1);

            return resultSet.wasNull();
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        if (!closed && !isNullOrEmpty(split.getSplitField()) && split.getPdboEnable()) {
            client.commitPdboLogs(split, rowRecord.get());
        }
        closed = true;
        // use try with resources to close everything properly
        try (ResultSet resultSet = this.resultSet;
                Statement statement = this.statement;
                Connection connection = this.connection) {
            // do nothing
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private Object getFieldValue(int field)
    {
        String lowerCase = columnHandles.get(field).getColumnName().toLowerCase();
        JdbcJavaBean jdbcJavaBean = tableDataSet.get(rowRecord.intValue() - 1);
        return jdbcJavaBean.getFieldObjectValue(jdbcJavaBean.getColumns().indexOf(lowerCase));
    }

    private RuntimeException handleSqlException(SQLException e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            e.addSuppressed(closeException);
        }
        return Throwables.propagate(e);
    }
}
