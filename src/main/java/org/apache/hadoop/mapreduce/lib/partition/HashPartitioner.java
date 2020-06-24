/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partition keys by their {@link Object#hashCode()}. sss mr 默认的Hash分区实现类*/
@InterfaceAudience.Public
@InterfaceStability.Stable //sss 定义在Partitioner中的泛型的KV是Mapper输出后的KV
public class HashPartitioner<K, V> extends Partitioner<K, V> {

  /** Use {@link Object#hashCode()} to partition. */ //sss getPartiton用来返回当前key,value在哪个分区中
  public int getPartition(K key, V value,
                          int numReduceTasks) {//numReduceTasks默认是1,下面算法是对numreducerTask取余,结果都是0
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}
