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
package org.apache.sedona.core.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.locationtech.jts.geom.Envelope;

/**
 * Selects spatial partitions that may hold geometries matching a range query window.
 *
 * <p>A spatial partitioner may place a geometry which spans several grid cells into a single cell
 * (see {@link GenericUniquePartitioner}). When a range query only scans the cells whose extents
 * intersect the query window, boundary-spanning geometries stored in non-intersecting cells are
 * silently dropped. This finder computes a recall-safe set of candidate partitions without touching
 * the existing partition layout or indexes:
 *
 * <ol>
 *   <li>every partition whose extent intersects the query window is selected;
 *   <li>every partition sharing a boundary with a selected partition is selected, because a
 *       geometry stored there may stretch across that shared boundary into the window;
 *   <li>the overflow container (if any) is always selected as it holds geometries not contained by
 *       any grid extent.
 * </ol>
 *
 * The exact spatial predicate is re-evaluated on every candidate geometry by the caller, so
 * scanning extra partitions only affects recall, never precision.
 */
public class PartitionCandidateFinder implements Serializable {

  private final List<Envelope> grids;
  private final int numPartitions;

  public PartitionCandidateFinder(SpatialPartitioner partitioner) {
    this.grids = Objects.requireNonNull(partitioner.getGrids(), "partitioner grids");
    this.numPartitions = partitioner.numPartitions();
  }

  /**
   * Returns the IDs of the partitions that must be scanned for the given query window.
   *
   * @param queryEnvelope envelope of the range query geometry
   * @return partition IDs in ascending order, never {@code null}
   */
  public List<Integer> findCandidatePartitions(Envelope queryEnvelope) {
    boolean[] selected = new boolean[numPartitions];
    List<Integer> intersectingPartitions = new ArrayList<>();

    for (int partitionId = 0; partitionId < grids.size(); partitionId++) {
      if (grids.get(partitionId).intersects(queryEnvelope)) {
        selected[partitionId] = true;
        intersectingPartitions.add(partitionId);
      }
    }

    // A geometry stored in a neighboring cell may stretch across the shared cell boundary and
    // intersect the query window even though its host cell itself does not intersect the window.
    for (int partitionId : intersectingPartitions) {
      Envelope extent = grids.get(partitionId);
      for (int neighborId = 0; neighborId < grids.size(); neighborId++) {
        if (!selected[neighborId] && extentsShareBoundary(extent, grids.get(neighborId))) {
          selected[neighborId] = true;
        }
      }
    }

    // The overflow container holds geometries not covered by any grid extent and must always be
    // considered when the partitioner provides one.
    if (numPartitions > grids.size()) {
      selected[grids.size()] = true;
    }

    List<Integer> result = new ArrayList<>();
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      if (selected[partitionId]) {
        result.add(partitionId);
      }
    }
    // Safety net: if no grid extent intersects the window and there is no overflow container,
    // fall back to scanning every partition so the query can never silently lose data.
    if (result.isEmpty()) {
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        result.add(partitionId);
      }
    }
    return result;
  }

  /**
   * Two extents share a boundary when they meet along a vertical or horizontal line segment (or at
   * a point) without overlapping in their interiors.
   */
  private static boolean extentsShareBoundary(Envelope first, Envelope second) {
    boolean touchOnX = first.getMaxX() == second.getMinX() || first.getMinX() == second.getMaxX();
    boolean overlapOnY = first.getMinY() <= second.getMaxY() && second.getMinY() <= first.getMaxY();
    boolean touchOnY = first.getMaxY() == second.getMinY() || first.getMinY() == second.getMaxY();
    boolean overlapOnX = first.getMinX() <= second.getMaxX() && second.getMinX() <= first.getMaxX();
    return (touchOnX && overlapOnY) || (touchOnY && overlapOnX);
  }
}
