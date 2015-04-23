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
package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Insert
        extends Statement
{
    private final QualifiedName target;
    private final Query query;
    private final boolean overwrite;
    private final boolean partition;
    private final List<PartitionElement> partitionElements;

    public Insert(QualifiedName target, Query query, boolean overwrite, boolean partition, List<PartitionElement> partitionValueList)
    {
        this.target = checkNotNull(target, "target is null");
        this.query = checkNotNull(query, "query is null");
        this.overwrite = overwrite;
        this.partition = partition;
        this.partitionElements = ImmutableList.copyOf(partitionValueList);
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public Query getQuery()
    {
        return query;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public boolean isPartition()
    {
        return partition;
    }

    public List<PartitionElement> getPartitionElements()
    {
        return partitionElements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInsert(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, query, overwrite, partition, partitionElements);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Insert o = (Insert) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(query, o.query) &&
                overwrite == o.overwrite &&
                partition == o.partition &&
                Objects.equals(partitionElements, o.partitionElements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("query", query)
                .add("overwrite", overwrite)
                .add("partition", partition)
                .add("partitionElements", partitionElements)
                .toString();
    }
}
