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

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

public class PartitionCandidateFinderTest {

  private List<Envelope> fourGrids() {
    List<Envelope> grids = new ArrayList<>();
    grids.add(new Envelope(0, 10, 0, 10));
    grids.add(new Envelope(10, 20, 0, 10));
    grids.add(new Envelope(0, 10, 10, 20));
    grids.add(new Envelope(10, 20, 10, 20));
    return grids;
  }

  @Test
  public void testWindowInsideOneCellSelectsCellAndItsNeighbors() {
    FlatGridPartitioner partitioner = new FlatGridPartitioner(fourGrids(), false);
    PartitionCandidateFinder finder = new PartitionCandidateFinder(partitioner);

    // Query window fully inside cell 0: cell 0 plus all cells sharing a boundary with it.
    List<Integer> candidates = finder.findCandidatePartitions(new Envelope(1, 2, 1, 2));

    Assert.assertEquals(4, candidates.size());
    Assert.assertTrue(candidates.contains(0));
    Assert.assertTrue(candidates.contains(1));
    Assert.assertTrue(candidates.contains(2));
    Assert.assertTrue(candidates.contains(3));
  }

  @Test
  public void testWindowOnCellBoundaryIncludesBothSides() {
    FlatGridPartitioner partitioner = new FlatGridPartitioner(fourGrids(), false);
    PartitionCandidateFinder finder = new PartitionCandidateFinder(partitioner);

    // Query window straddles the vertical boundary at x=10.
    List<Integer> candidates = finder.findCandidatePartitions(new Envelope(9, 11, 1, 2));

    Assert.assertEquals(4, candidates.size());
  }

  @Test
  public void testWindowCoveringAllGridsSelectsAllPartitions() {
    FlatGridPartitioner partitioner = new FlatGridPartitioner(fourGrids(), false);
    PartitionCandidateFinder finder = new PartitionCandidateFinder(partitioner);

    List<Integer> candidates = finder.findCandidatePartitions(new Envelope(0, 20, 0, 20));

    Assert.assertEquals(4, candidates.size());
  }

  @Test
  public void testOverflowPartitionAlwaysSelected() {
    // preserveUncontainedGeometries=true adds an overflow container with partition ID 4.
    FlatGridPartitioner partitioner = new FlatGridPartitioner(fourGrids(), true);
    PartitionCandidateFinder finder = new PartitionCandidateFinder(partitioner);

    List<Integer> candidates = finder.findCandidatePartitions(new Envelope(1, 2, 1, 2));

    Assert.assertTrue(candidates.contains(4));
    Assert.assertEquals(5, candidates.size());
  }

  @Test
  public void testWindowOutsideAllGridsStillKeepsOverflow() {
    FlatGridPartitioner partitioner = new FlatGridPartitioner(fourGrids(), true);
    PartitionCandidateFinder finder = new PartitionCandidateFinder(partitioner);

    List<Integer> candidates = finder.findCandidatePartitions(new Envelope(100, 110, 100, 110));

    // No grid intersects the window, but the overflow container may still hold matching data.
    Assert.assertEquals(1, candidates.size());
    Assert.assertTrue(candidates.contains(4));
  }
}
