/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.rangeJudgement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

/**
 * Range filter for spatially partitioned RDDs carrying a per-partition {@link SpatialIndex}.
 *
 * <p>Partitions outside the recall-safe candidate set computed by {@link
 * org.apache.sedona.core.spatialPartitioning.PartitionCandidateFinder} are skipped without touching
 * their indexes. The remaining partitions use the index for an envelope pre-filter and then apply
 * the exact spatial predicate, identical to {@link RangeFilterUsingIndex}.
 */
public class PartitionAwareRangeFilterUsingIndex<U extends Geometry, T extends Geometry>
    extends JudgementBase implements Function2<Integer, Iterator<SpatialIndex>, Iterator<T>> {

  private final Set<Integer> candidatePartitions;

  public PartitionAwareRangeFilterUsingIndex(
      U queryWindow, SpatialPredicate spatialPredicate, Set<Integer> candidatePartitions) {
    super(queryWindow, spatialPredicate);
    this.candidatePartitions = candidatePartitions;
  }

  @Override
  public Iterator<T> call(Integer partitionId, Iterator<SpatialIndex> treeIndexes) {
    if (!candidatePartitions.contains(partitionId)) {
      return new ArrayList<T>().iterator();
    }
    assert treeIndexes.hasNext();
    SpatialIndex treeIndex = treeIndexes.next();
    List<T> results = new ArrayList<>();
    List<T> tempResults = treeIndex.query(this.queryGeometry.getEnvelopeInternal());
    for (T tempResult : tempResults) {
      if (match(tempResult, queryGeometry)) {
        results.add(tempResult);
      }
    }
    return results.iterator();
  }
}
