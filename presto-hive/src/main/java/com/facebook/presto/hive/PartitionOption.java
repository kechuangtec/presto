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

import java.util.LinkedHashMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionOption
{
    private final boolean dynamicPartition;
    private final LinkedHashMap<String, String> partitionElements;
    private final String partitionSuffix;

    @JsonCreator
    public PartitionOption(
            @JsonProperty("dynamicPartition") boolean dynamicPartition,
            @JsonProperty("partitionElements") LinkedHashMap<String, String> partitionElements,
            @JsonProperty("partitionSuffix") String partitionSuffix)
    {
        this.dynamicPartition = dynamicPartition;
        this.partitionElements = new LinkedHashMap<String, String>(partitionElements);
        this.partitionSuffix = partitionSuffix;
    }

    @JsonProperty
    public String getPartitionSuffix()
    {
        return partitionSuffix;
    }

    @JsonProperty
    public LinkedHashMap<String, String> getPartitionElements()
    {
        return partitionElements;
    }

    @JsonProperty
    public boolean isDynamicPartition()
    {
        return dynamicPartition;
    }
}
