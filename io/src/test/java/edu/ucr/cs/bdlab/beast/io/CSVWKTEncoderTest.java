package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

public class CSVWKTEncoderTest extends JavaSparkTest {

  public void testEncodePointWithAttributes() {
    PointND p = new PointND(new GeometryFactory(), 2, 0.5, 0.1);
    Feature f = new Feature(p, null, null, new Object[] {"att1", "att2", "att3", "att4"});
    char fieldSeparator = ',';
    CSVWKTEncoder writer = new CSVWKTEncoder(fieldSeparator, 1);
    String s = writer.apply(f, null).toString();
    assertEquals("att1,POINT(0.5 0.1),att2,att3,att4", s);
  }
}