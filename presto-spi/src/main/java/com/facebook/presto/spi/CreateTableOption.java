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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.List;

public class CreateTableOption
{
    private final boolean isPartition;
    private final List<String> partitions;

    public CreateTableOption(boolean isPartition, List<String> partitions)
    {
        this.isPartition = isPartition;
        if (partitions == null) {
            partitions = new ArrayList<>();
        }
        this.partitions = new ArrayList<>(partitions);
    }

    @Override
    public String toString()
    {
        return partitions.toString();
    }

    public boolean isPartition()
    {
        return isPartition;
    }

    public List<String> getPartitions()
    {
        return partitions;
    }
}
