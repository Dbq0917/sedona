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
package org.apache.sedona.core.spatialOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialPartitioning.FlatGridPartitioner;
import org.apache.sedona.core.spatialPartitioning.GenericUniquePartitioner;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTReader;

public class PartitionedRangeQueryTest extends TestBase {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private static final List<Envelope> GRIDS =
      Arrays.asList(
          new Envelope(0, 10, 0, 10),
          new Envelope(10, 20, 0, 10),
          new Envelope(0, 10, 10, 20),
          new Envelope(10, 20, 10, 20));

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize(PartitionedRangeQueryTest.class.getSimpleName());
  }

  @AfterClass
  public static void tearDown() {
    sc.stop();
  }

  private SpatialRDD<Geometry> uniquePartitionedRDD() throws Exception {
    List<String> wkts = new ArrayList<>();
    // Spans all four cells but is stored in exactly one cell by the unique partitioner.
    wkts.add("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))");
    // Sits fully inside cell 1 (10-20, 0-10).
    wkts.add("POLYGON ((12 2, 14 2, 14 4, 12 4, 12 2))");
    // Point on the shared boundary at x=10.
    wkts.add("POINT (10 5)");

    JavaRDD<Geometry> rawRdd =
        sc.parallelize(wkts)
            .map(
                wkt -> {
                  try {
                    return new WKTReader().read(wkt);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = rawRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioningWithoutDuplicates(
        new GenericUniquePartitioner(new FlatGridPartitioner(GRIDS, false)));
    return spatialRDD;
  }

  private long expectedIntersectCount(Envelope queryWindow) throws Exception {
    Geometry queryGeometry = GEOMETRY_FACTORY.toGeometry(queryWindow);
    return uniquePartitionedRDD()
        .getRawSpatialRDD()
        .filter(geom -> geom.intersects(queryGeometry))
        .count();
  }

  @Test
  public void testRangeQueryOnUniquePartitionedRDDWithoutIndex() throws Exception {
    Envelope queryWindow = new Envelope(10.5, 11.5, 10.5, 11.5);
    long expected = expectedIntersectCount(queryWindow);

    SpatialRDD<Geometry> spatialRDD = uniquePartitionedRDD();
    JavaRDD<Geometry> result =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, SpatialPredicate.INTERSECTS, false);

    Assert.assertEquals(expected, result.count());
    // The cross-cell polygon physically lives in cell 0, whose extent is disjoint from the
    // window, so it is exactly the boundary object a naive cell-intersection pruning would miss.
    Assert.assertEquals(1, result.count());
  }

  @Test
  public void testRangeQueryOnUniquePartitionedRDDWithIndex() throws Exception {
    Envelope queryWindow = new Envelope(10.5, 11.5, 10.5, 11.5);
    long expected = expectedIntersectCount(queryWindow);

    SpatialRDD<Geometry> spatialRDD = uniquePartitionedRDD();
    spatialRDD.buildIndex(IndexType.RTREE, true);
    JavaRDD<Geometry> result =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, SpatialPredicate.INTERSECTS, true);

    Assert.assertEquals(expected, result.count());
    Assert.assertEquals(1, result.count());
  }

  @Test
  public void testPointOnCellBoundaryIsReturned() throws Exception {
    Envelope queryWindow = new Envelope(10, 12, 4, 6);

    SpatialRDD<Geometry> spatialRDD = uniquePartitionedRDD();
    JavaRDD<Geometry> result =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, SpatialPredicate.INTERSECTS, false);

    Assert.assertEquals(expectedIntersectCount(queryWindow), result.count());
    Assert.assertTrue(result.count() >= 2);
  }

  @Test
  public void testQueryWindowOutsideDataReturnsNothing() throws Exception {
    Envelope queryWindow = new Envelope(40, 50, 40, 50);

    SpatialRDD<Geometry> spatialRDD = uniquePartitionedRDD();
    JavaRDD<Geometry> result =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, SpatialPredicate.INTERSECTS, false);

    Assert.assertEquals(0, result.count());
  }

  @Test
  public void testDuplicatingPartitionerKeepsLegacyRawPath() throws Exception {
    // The regular FlatGridPartitioner duplicates cross-cell geometries into every intersecting
    // cell. RangeQuery must keep using the raw RDD path for it instead of the unique-partitioner
    // path, so existing behavior is preserved regardless of partitioning.
    Envelope queryWindow = new Envelope(10.5, 11.5, 10.5, 11.5);
    Geometry queryGeometry = GEOMETRY_FACTORY.toGeometry(queryWindow);

    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = uniquePartitionedRDD().getRawSpatialRDD();
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(new FlatGridPartitioner(GRIDS, false));

    JavaRDD<Geometry> result =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, SpatialPredicate.INTERSECTS, false);

    long expected =
        spatialRDD.getRawSpatialRDD().filter(geom -> geom.intersects(queryGeometry)).count();
    Assert.assertEquals(expected, result.count());
  }
}
