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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.CreateTableOption;
import com.facebook.presto.spi.InsertOption;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.PartitionElement;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.util.DateTimeUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.aliasedName;
import static com.facebook.presto.sql.QueryUtil.aliasedNullToEmpty;
import static com.facebook.presto.sql.QueryUtil.aliasedYesNoToBoolean;
import static com.facebook.presto.sql.QueryUtil.ascending;
import static com.facebook.presto.sql.QueryUtil.caseWhen;
import static com.facebook.presto.sql.QueryUtil.equal;
import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.sql.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.ordering;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectAll;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.subquery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.QueryUtil.unaliasedName;
import static com.facebook.presto.sql.QueryUtil.values;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.UNSUPPORTED_PARTITION_TYPE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARTITION_VALUE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_PARTITION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_PARTITION_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PARTITION_NAME;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ExplainFormat.Type.TEXT;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.contains;
import static java.util.stream.Collectors.toList;

class StatementAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final boolean experimentalSyntaxEnabled;
    private final SqlParser sqlParser;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            Session session,
            boolean experimentalSyntaxEnabled,
            Optional<QueryExplainer> queryExplainer)
    {
        this.analysis = checkNotNull(analysis, "analysis is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
        this.session = checkNotNull(session, "session is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
        this.queryExplainer = checkNotNull(queryExplainer, "queryExplainer is null");
    }

    @Override
    protected TupleDescriptor visitShowTables(ShowTables showTables, AnalysisContext context)
    {
        String catalogName = session.getCatalog();
        String schemaName = session.getSchema();

        Optional<QualifiedName> schema = showTables.getSchema();
        if (schema.isPresent()) {
            List<String> parts = schema.get().getParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, showTables, "too many parts in schema name: %s", schema);
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.get().getSuffix();
        }

        // TODO: throw SemanticException if schema does not exist

        Expression predicate = equal(nameReference("table_schema"), new StringLiteral(schemaName));

        Optional<String> likePattern = showTables.getLikePattern();
        if (likePattern.isPresent()) {
            Expression likePredicate = new LikePredicate(nameReference("table_name"), new StringLiteral(likePattern.get()), null);
            predicate = logicalAnd(predicate, likePredicate);
        }

        Query query = simpleQuery(
                selectList(aliasedName("table_name", "Table")),
                from(catalogName, TABLE_TABLES),
                predicate,
                ordering(ascending("table_name")));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowSchemas(ShowSchemas node, AnalysisContext context)
    {
        Query query = simpleQuery(
                selectList(aliasedName("schema_name", "Schema")),
                from(node.getCatalog().orElse(session.getCatalog()), TABLE_SCHEMATA),
                ordering(ascending("schema_name")));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowCatalogs(ShowCatalogs node, AnalysisContext context)
    {
        List<Expression> rows = metadata.getCatalogNames().keySet().stream()
                .map(name -> row(new StringLiteral(name)))
                .collect(toList());

        Query query = simpleQuery(
                selectList(new AllColumns()),
                aliased(new Values(rows), "catalogs", ImmutableList.of("Catalog")));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowColumns(ShowColumns showColumns, AnalysisContext context)
    {
        QualifiedTableName tableName = MetadataUtil.createQualifiedTableName(session, showColumns.getTable());

        if (!metadata.getView(session, tableName).isPresent() &&
                !metadata.getTableHandle(session, tableName).isPresent()) {
            throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
        }

        Query query = simpleQuery(
                selectList(
                        aliasedName("column_name", "Column"),
                        aliasedName("data_type", "Type"),
                        aliasedYesNoToBoolean("is_nullable", "Null"),
                        aliasedYesNoToBoolean("is_partition_key", "Partition Key"),
                        aliasedNullToEmpty("comment", "Comment")),
                from(tableName.getCatalogName(), TABLE_COLUMNS),
                logicalAnd(
                        equal(nameReference("table_schema"), new StringLiteral(tableName.getSchemaName())),
                        equal(nameReference("table_name"), new StringLiteral(tableName.getTableName()))),
                ordering(ascending("ordinal_position")));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitUse(Use node, AnalysisContext context)
    {
        analysis.setUpdateType("USE");
        throw new SemanticException(NOT_SUPPORTED, node, "USE statement is not supported");
    }

    @Override
    protected TupleDescriptor visitShowPartitions(ShowPartitions showPartitions, AnalysisContext context)
    {
        QualifiedTableName table = MetadataUtil.createQualifiedTableName(session, showPartitions.getTable());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, table);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, showPartitions, "Table '%s' does not exist", table);
        }

            /*
                Generate a dynamic pivot to output one column per partition key.
                For example, a table with two partition keys (ds, cluster_name)
                would generate the following query:

                SELECT
                  partition_number
                , max(CASE WHEN partition_key = 'ds' THEN partition_value END) ds
                , max(CASE WHEN partition_key = 'cluster_name' THEN partition_value END) cluster_name
                FROM ...
                GROUP BY partition_number

                The values are also cast to the type of the partition column.
                The query is then wrapped to allow custom filtering and ordering.
            */

        ImmutableList.Builder<SelectItem> selectList = ImmutableList.builder();
        ImmutableList.Builder<SelectItem> wrappedList = ImmutableList.builder();
        selectList.add(unaliasedName("partition_number"));
        for (ColumnMetadata column : metadata.getTableMetadata(tableHandle.get()).getColumns()) {
            if (!column.isPartitionKey()) {
                continue;
            }
            Expression key = equal(nameReference("partition_key"), new StringLiteral(column.getName()));
            Expression value = caseWhen(key, nameReference("partition_value"));
            value = new Cast(value, column.getType().getTypeSignature().toString());
            Expression function = functionCall("max", value);
            selectList.add(new SingleColumn(function, column.getName()));
            wrappedList.add(unaliasedName(column.getName()));
        }

        Query query = simpleQuery(
                selectAll(selectList.build()),
                from(table.getCatalogName(), TABLE_INTERNAL_PARTITIONS),
                Optional.of(logicalAnd(
                        equal(nameReference("table_schema"), new StringLiteral(table.getSchemaName())),
                        equal(nameReference("table_name"), new StringLiteral(table.getTableName())))),
                ImmutableList.of(nameReference("partition_number")),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());

        query = simpleQuery(
                selectAll(wrappedList.build()),
                subquery(query),
                showPartitions.getWhere(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.<SortItem>builder()
                        .addAll(showPartitions.getOrderBy())
                        .add(ascending("partition_number"))
                        .build(),
                showPartitions.getLimit());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowFunctions(ShowFunctions node, AnalysisContext context)
    {
        Query query = simpleQuery(selectList(
                        aliasedName("function_name", "Function"),
                        aliasedName("return_type", "Return Type"),
                        aliasedName("argument_types", "Argument Types"),
                        aliasedName("function_type", "Function Type"),
                        aliasedName("deterministic", "Deterministic"),
                        aliasedName("description", "Description")),
                from(session.getCatalog(), TABLE_INTERNAL_FUNCTIONS),
                ordering(
                        ascending("function_name"),
                        ascending("return_type"),
                        ascending("argument_types"),
                        ascending("function_type")));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowSession(ShowSession node, AnalysisContext context)
    {
        ImmutableList.Builder<Expression> rows = ImmutableList.builder();
        for (Entry<String, String> property : new TreeMap<>(session.getSystemProperties()).entrySet()) {
            rows.add(row(
                    new StringLiteral(property.getKey()),
                    new StringLiteral(property.getValue()),
                    TRUE_LITERAL));
        }
        for (Entry<String, Map<String, String>> entry : new TreeMap<>(session.getCatalogProperties()).entrySet()) {
            String catalog = entry.getKey();
            for (Entry<String, String> property : new TreeMap<>(entry.getValue()).entrySet()) {
                rows.add(row(
                        new StringLiteral(catalog + "." + property.getKey()),
                        new StringLiteral(property.getValue()),
                        TRUE_LITERAL));
            }
        }

        // add bogus row so we can support empty sessions
        rows.add(row(new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));

        Query query = simpleQuery(
                selectList(
                        aliasedName("name", "Name"),
                        aliasedName("value", "Value")),
                aliased(
                        new Values(rows.build()),
                        "session",
                        ImmutableList.of("name", "value", "include")),
                nameReference("include"));

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitInsert(Insert insert, AnalysisContext context)
    {
        analysis.setUpdateType("INSERT");

        // analyze the query that creates the data
        TupleDescriptor descriptor = process(insert.getQuery(), context);

        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, insert.getTarget());
        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
        if (!targetTableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, insert, "Table '%s' does not exist", targetTable);
        }
        analysis.setInsertTarget(targetTableHandle.get());

        List<ColumnMetadata> columns = metadata.getTableMetadata(targetTableHandle.get()).getColumns();

        analyzeInsertOption(insert, columns, targetTable);

        // verify the insert destination columns match the query
        Iterable<Type> tableTypes = FluentIterable.from(columns)
                .filter(column -> !column.isHidden())
                .transform(ColumnMetadata::getType);

        Iterable<Type> queryTypes = transform(descriptor.getVisibleFields(), Field::getType);

        if (!elementsEqual(tableTypes, queryTypes)) {
            throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES, insert, "Insert query has mismatched column types: " +
                    "Table: (" + Joiner.on(", ").join(tableTypes) + "), " +
                    "Query: (" + Joiner.on(", ").join(queryTypes) + ")");
        }

        return new TupleDescriptor(Field.newUnqualified("rows", BIGINT));
    }

    private void analyzeInsertOption(Insert insert, List<ColumnMetadata> columns, QualifiedTableName targetTable)
    {
        boolean dynamicPartition = false;
        List<String> targetPartitionNames = columns.stream()
                .filter(column -> column.isPartitionKey())
                .map(ColumnMetadata::getName)
                .collect(toList());
        List<Type> targetPartitionTypes = columns.stream()
                .filter(column -> column.isPartitionKey())
                .map(ColumnMetadata::getType)
                .collect(toList());

        if (!targetPartitionNames.isEmpty() && !insert.isPartition()) {
            throw new SemanticException(MISSING_PARTITION, insert, "Target table '%s' is partitioned but insert query does not specifty the partition", targetTable);
        }

        if (insert.isPartition() && targetPartitionNames.isEmpty()) {
            throw new SemanticException(MISSING_PARTITION, insert, "Target table '%s' is not partitioned but insert query speciftys the partition", targetTable);
        }

        HashMap<String, String> partitionKeyValue = new HashMap<String, String>();
        HashMap<String, String> partitionTypes = new HashMap<String, String>();
        LinkedHashMap<String, String> partitionKeyValueInorder = new LinkedHashMap<String, String>();

        if (insert.isPartition()) {
            dynamicPartition = analyzePartitionElements(insert, targetTable, partitionKeyValue, partitionTypes);
        }
        if (partitionKeyValue.size() != targetPartitionNames.size()) {
            throw new SemanticException(MISMATCHED_PARTITION_NAME, insert, "Insert query has mismatched partition names", targetTable);
        }
        else {
            int index = 0;
            for (Iterator<String> it = targetPartitionNames.iterator(); it.hasNext(); ) {
                String partitionName = it.next();
                if (!partitionKeyValue.containsKey(partitionName)) {
                    throw new SemanticException(MISMATCHED_PARTITION_NAME, insert, "Insert query has mismatched partition names", targetTable);
                }

                if (!dynamicPartition) {
                    String targetPartitionType = targetPartitionTypes.get(index++).getTypeSignature().toString();
                    String insertPartitionType = partitionTypes.get(partitionName);
                    if (!insertPartitionType.equalsIgnoreCase(targetPartitionType)) {
                        throw new SemanticException(INVALID_PARTITION_VALUE,
                                insert, "Insert query has invalid partition value, "
                                        + "partition " + partitionName
                                        + " should be of type "
                                        + targetPartitionType + " but is of type "
                                        + insertPartitionType, targetTable);
                    }
                }
                // reorder the partitionKeyValue as the order of select query order
                partitionKeyValueInorder.put(partitionName, partitionKeyValue.get(partitionName));
            }
        }
        analysis.setInsertOption(new InsertOption(insert.isOverwrite(), insert.isPartition(), dynamicPartition, partitionKeyValueInorder));
    }

    private boolean analyzePartitionElements(Insert insert, QualifiedTableName targetTable, HashMap<String, String> partitionKeyValue, HashMap<String, String> partitionTypes)
    {
        boolean dynamicPartition = false;
        for (PartitionElement element : insert.getPartitionElements()) {
            String partitionName = element.getName();
            String partitionValue = "";
            String partitionType = "";
            if (!element.getValue().isPresent()) {
                dynamicPartition = true;
                partitionTypes.put(partitionName, partitionType);
                partitionKeyValue.put(partitionName, partitionValue);
                continue;
            }
            Expression exp = element.getValue().get();

            if (partitionKeyValue.containsKey(partitionName)) {
                throw new SemanticException(DUPLICATE_PARTITION_NAME, insert, "Insert query has duplicate partition names", targetTable);
            }

            if (exp instanceof BooleanLiteral) {
                partitionType = "boolean";
                partitionValue = String.valueOf(((BooleanLiteral) exp).getValue());
            }
            else if (exp instanceof DoubleLiteral) {
                partitionType = "double";
                partitionValue = String.valueOf(((DoubleLiteral) exp).getValue());
            }
            else if (exp instanceof LongLiteral) {
                partitionType = "bigint";
                partitionValue = String.valueOf(((LongLiteral) exp).getValue());
            }
            else if (exp instanceof StringLiteral) {
                partitionType = "varchar";
                partitionValue = ((StringLiteral) exp).getValue();
            }
            else if (exp instanceof TimeLiteral) {
                partitionType = "time";
                long value = DateTimeUtils.parseTime(session.getTimeZoneKey(), ((TimeLiteral) exp).getValue());
                partitionValue = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date(value));
            }
            else if (exp instanceof TimestampLiteral) {
                partitionType = "timestamp";
                long value = DateTimeUtils.parseTimestamp(session.getTimeZoneKey(), ((TimestampLiteral) exp).getValue());
                partitionValue = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(value));
            }
            else if (exp instanceof GenericLiteral) {
                GenericLiteral genericLiteral = (GenericLiteral) exp;
                partitionType = genericLiteral.getType();
                partitionValue = genericLiteral.getValue();
                if (!isPartitionTypeSupported(partitionType)) {
                    throw new SemanticException(UNSUPPORTED_PARTITION_TYPE, insert, "Unsupported partition type", targetTable);
                }
                if (partitionType.equalsIgnoreCase("time")) {
                    long value = DateTimeUtils.parseTime(session.getTimeZoneKey(), partitionValue);
                    partitionValue = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date(value));
                }
                else if (partitionType.equalsIgnoreCase("timestamp")) {
                    long value = DateTimeUtils.parseTimestamp(session.getTimeZoneKey(), partitionValue);
                    partitionValue = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(value));
                }
            }
            else {
                throw new SemanticException(UNSUPPORTED_PARTITION_TYPE, insert, "Unsupported partition type", targetTable);
            }

            partitionTypes.put(partitionName, partitionType);
            partitionKeyValue.put(partitionName, partitionValue);
        }
        return dynamicPartition;
    }

    private boolean isPartitionTypeSupported(String type)
    {
        if (type.equalsIgnoreCase("boolean") ||
                type.equalsIgnoreCase("double") ||
                type.equalsIgnoreCase("bigint") ||
                type.equalsIgnoreCase("varchar") ||
                type.equalsIgnoreCase("date") ||
                type.equalsIgnoreCase("time") ||
                type.equalsIgnoreCase("timestamp")) {
            return true;
        }
        return false;
    }

    @Override
    protected TupleDescriptor visitCreateTableAsSelect(CreateTableAsSelect node, AnalysisContext context)
    {
        analysis.setUpdateType("CREATE TABLE");

        // turn this into a query that has a new table writer node on top.
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        analysis.setCreateTableDestination(targetTable);

        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
        if (targetTableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
        }

        // analyze the query that creates the table
        TupleDescriptor descriptor = process(node.getQuery(), context);

        validateColumnNames(node, descriptor);

        // @author : yuananf
        // handle partition exceptions
        if (node.isPartition()) {
            Iterable<Optional<String>> queryColumns = transform(descriptor.getVisibleFields(), Field::getName);
            for (String partitionColumn : node.getPartitionList()) {
            if (!contains(queryColumns, Optional.of(partitionColumn))) {
                    throw new SemanticException(MISSING_PARTITION, node, "Target table '%s' is partitioned but query does not speciftys the partition", targetTable);
                }
            }
        }
        analysis.setCreateTableOption(new CreateTableOption(node.isPartition(), node.getPartitionList()));

        return new TupleDescriptor(Field.newUnqualified("rows", BIGINT));
    }

    @Override
    protected TupleDescriptor visitCreateView(CreateView node, AnalysisContext context)
    {
        analysis.setUpdateType("CREATE VIEW");

        // analyze the query that creates the view
        TupleDescriptor descriptor = process(node.getQuery(), context);

        validateColumnNames(node, descriptor);

        return descriptor;
    }

    private static void validateColumnNames(Statement node, TupleDescriptor descriptor)
    {
        // verify that all column names are specified and unique
        // TODO: collect errors and return them all at once
        Set<String> names = new HashSet<>();
        for (Field field : descriptor.getVisibleFields()) {
            Optional<String> fieldName = field.getName();
            if (!fieldName.isPresent()) {
                throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, node, "Column name not specified at position %s", descriptor.indexOf(field) + 1);
            }
            if (!names.add(fieldName.get())) {
                throw new SemanticException(DUPLICATE_COLUMN_NAME, node, "Column name '%s' specified more than once", fieldName.get());
            }
        }
    }

    @Override
    protected TupleDescriptor visitExplain(Explain node, AnalysisContext context)
            throws SemanticException
    {
        checkState(queryExplainer.isPresent(), "query explainer not available");
        ExplainType.Type planType = LOGICAL;
        ExplainFormat.Type planFormat = TEXT;
        List<ExplainOption> options = node.getOptions();

        for (ExplainOption option : options) {
            if (option instanceof ExplainType) {
                planType = ((ExplainType) option).getType();
                break;
            }
        }

        for (ExplainOption option : options) {
            if (option instanceof ExplainFormat) {
                planFormat = ((ExplainFormat) option).getType();
                break;
            }
        }

        String queryPlan = getQueryPlan(node, planType, planFormat);

        Query query = simpleQuery(
                selectList(new AllColumns()),
                aliased(
                        values(row(new StringLiteral((queryPlan)))),
                        "plan",
                        ImmutableList.of("Query Plan")));

        return process(query, context);
    }

    private String getQueryPlan(Explain node, ExplainType.Type planType, ExplainFormat.Type planFormat)
            throws IllegalArgumentException
    {
        switch (planFormat) {
            case GRAPHVIZ:
                return queryExplainer.get().getGraphvizPlan(node.getStatement(), planType);
            case TEXT:
                return queryExplainer.get().getPlan(node.getStatement(), planType);
            case JSON:
                // ignore planType if planFormat is JSON
                return queryExplainer.get().getJsonPlan(node.getStatement());
        }
        throw new IllegalArgumentException("Invalid Explain Format: " + planFormat.toString());
    }

    @Override
    protected TupleDescriptor visitQuery(Query node, AnalysisContext parentContext)
    {
        AnalysisContext context = new AnalysisContext(parentContext);

        if (node.getApproximate().isPresent()) {
            if (!experimentalSyntaxEnabled) {
                throw new SemanticException(NOT_SUPPORTED, node, "approximate queries are not enabled");
            }
            context.setApproximate(true);
        }

        analyzeWith(node, context);

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata, sqlParser, experimentalSyntaxEnabled);
        TupleDescriptor descriptor = analyzer.process(node.getQueryBody(), context);
        analyzeOrderBy(node, descriptor, context);

        // Input fields == Output fields
        analysis.setOutputDescriptor(node, descriptor);
        analysis.setOutputExpressions(node, descriptorToFields(descriptor));
        analysis.setQuery(node);

        return descriptor;
    }

    private static List<FieldOrExpression> descriptorToFields(TupleDescriptor tupleDescriptor)
    {
        ImmutableList.Builder<FieldOrExpression> builder = ImmutableList.builder();
        for (int fieldIndex = 0; fieldIndex < tupleDescriptor.getAllFieldCount(); fieldIndex++) {
            builder.add(new FieldOrExpression(fieldIndex));
        }
        return builder.build();
    }

    private void analyzeWith(Query node, AnalysisContext context)
    {
        // analyze WITH clause
        if (!node.getWith().isPresent()) {
            return;
        }

        With with = node.getWith().get();
        if (with.isRecursive()) {
            throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
        }

        for (WithQuery withQuery : with.getQueries()) {
            if (withQuery.getColumnNames() != null && !withQuery.getColumnNames().isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, withQuery, "Column alias not supported in WITH queries");
            }

            Query query = withQuery.getQuery();
            process(query, context);

            String name = withQuery.getName();
            if (context.isNamedQueryDeclared(name)) {
                throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
            }

            context.addNamedQuery(name, query);
        }
    }

    private void analyzeOrderBy(Query node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByFieldsBuilder = ImmutableList.builder();

        if (!items.isEmpty()) {
            for (SortItem item : items) {
                Expression expression = item.getSortKey();

                FieldOrExpression orderByField;
                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > tupleDescriptor.getVisibleFieldCount()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByField = new FieldOrExpression(Ints.checkedCast(ordinal - 1));
                }
                else {
                    // otherwise, just use the expression as is
                    orderByField = new FieldOrExpression(expression);
                    ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(session,
                            metadata,
                            sqlParser,
                            tupleDescriptor,
                            analysis,
                            experimentalSyntaxEnabled,
                            context,
                            orderByField.getExpression());
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                }

                orderByFieldsBuilder.add(orderByField);
            }
        }

        analysis.setOrderByExpressions(node, orderByFieldsBuilder.build());
    }

    private static Relation from(String catalog, SchemaTableName table)
    {
        return table(QualifiedName.of(catalog, table.getSchemaName(), table.getTableName()));
    }
}
