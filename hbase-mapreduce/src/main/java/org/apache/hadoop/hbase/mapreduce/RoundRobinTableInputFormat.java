/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
@InterfaceAudience.Public
public class RoundRobinTableInputFormat extends TableInputFormat {
  @VisibleForTesting
  static List<InputSplit> getRoundRobinSplits(List<InputSplit> splits) {
    Map<String, List<InputSplit>> regionServersInputSplits = new HashMap<>();

    for (InputSplit is : splits) {
      List<InputSplit> isList = regionServersInputSplits
        .computeIfAbsent(((TableSplit) is).getRegionLocation(), k -> new ArrayList<>());
      isList.add(is);
    }

    List<InputSplit> roundRobinSplits = new ArrayList<>();
    while (!regionServersInputSplits.isEmpty()) {
      Iterator<Map.Entry<String, List<InputSplit>>> it =
        regionServersInputSplits.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, List<InputSplit>> entry = it.next();
        roundRobinSplits.add(entry.getValue().remove(0));
        if (entry.getValue().isEmpty()) {
          it.remove();
        }
      }
    }
    return roundRobinSplits;
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table. Splits are sorted
   * in a round robin way of region servers.
   *
   * @param context The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *JobContext)
   */
  @Override public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = super.getSplits(context);

    if ((splits == null) || splits.isEmpty()) {
      return splits;
    }

    // For TableInputFormat, it should be TableSplit.
    if (splits.get(0) instanceof TableSplit) {
      return getRoundRobinSplits(splits);
    }

    // Return the original list if InputSplit is not TableSplit.
    return splits;
  }
}
