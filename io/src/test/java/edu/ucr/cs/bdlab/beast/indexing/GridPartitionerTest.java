package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.util.IntArray;
import junit.framework.TestCase;
import org.locationtech.jts.geom.GeometryFactory;

public class GridPartitionerTest extends TestCase {
  public void testOutOfBounds() {
    EnvelopeNDLite mbr = new EnvelopeNDLite(2, 0.0, 0.0, 0.9, 0.9);
    GridPartitioner g = new GridPartitioner(mbr, new int[] {92, 97});
    EnvelopeND e = new EnvelopeND(new GeometryFactory(), 2, 0.91, 0.91, 0.92, 0.92);
    IntArray overlappingResults = new IntArray();
    g.overlapPartitions(e, overlappingResults);
    assertEquals(0, overlappingResults.size());
  }
}