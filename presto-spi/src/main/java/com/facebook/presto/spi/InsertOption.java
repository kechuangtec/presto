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

import java.util.LinkedHashMap;

public class InsertOption
{
    private final boolean overwrite;
    private final boolean isPartition;
    private final boolean dynamicPartition;
    private final LinkedHashMap<String, String> partitions;

    public InsertOption(boolean overwrite, boolean isPartition, boolean dynamicPartition, LinkedHashMap<String, String> partitions)
    {
        this.overwrite = overwrite;
        this.isPartition = isPartition;
        this.dynamicPartition = dynamicPartition;
        if (partitions == null) {
            partitions = new LinkedHashMap<>();
        }
        this.partitions = new LinkedHashMap<>(partitions);
    }

    @Override
    public String toString()
    {
        return partitions.toString();
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public boolean isPartition()
    {
        return isPartition;
    }

    public boolean isDynamicPartition()
    {
        return dynamicPartition;
    }

    public LinkedHashMap<String, String> getPartitions()
    {
        return partitions;
    }
}
