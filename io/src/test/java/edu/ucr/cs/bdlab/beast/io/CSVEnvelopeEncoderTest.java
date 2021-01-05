package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

public class CSVEnvelopeEncoderTest extends JavaSparkTest {

  public void testEncode2DEnvelope() {
    IFeature feature = Feature.create(
        new EnvelopeND(new GeometryFactory(),2, 1.0, 2.0, 5.0, 3.0),
        null, null,
        new Object[] {"abc", "def"}
    );
    CSVEnvelopeEncoder encoder = new CSVEnvelopeEncoder(',', new int[] {1,2,4,5});
    assertArrayEquals(new int[] {-1,0,1,-1,2,3}, encoder.orderedColumns);
    String encoded = encoder.apply(feature, new StringBuilder()).toString();
    assertEquals("abc,1.0,2.0,def,5.0,3.0", encoded);
  }
}