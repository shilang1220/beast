package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.GeometryFactory;

public class CSVPointEncoderTest extends JavaSparkTest {
  public static GeometryFactory geometryFactory = new GeometryFactory();

  public void testPointWriterNoFeatureAttributes() {
    PointND p = new PointND(geometryFactory, 4, 0.5, 0.1, 1.2, 3.5);
    Feature f = new Feature(p);
    CSVPointEncoder writer = new CSVPointEncoder('\t', 0, 1, 2, 3);
    String s = writer.apply(f, null).toString();
    assertEquals("0.5\t0.1\t1.2\t3.5", s);
  }

  public void testPointWriterWithFeatureAttributes() {
    PointND p = new PointND(geometryFactory, 4, 0.5, 0.1, 1.2, 3.5);
    Feature f = new Feature(p, null, null, new Object[] {"att1", "att2", "att3", "att4"});
    CSVPointEncoder writer = new CSVPointEncoder(',', 1, 2, 5, 6);
    String s = writer.apply(f, null).toString();
    assertEquals("att1,0.5,0.1,att2,att3,1.2,3.5,att4", s);
  }

  public void testEmptyPoint() {
    PointND p = new PointND(geometryFactory);
    Feature f = new Feature(p);
    CSVPointEncoder writer = new CSVPointEncoder('\t', 0, 1);
    String s = writer.apply(f, null).toString();
    assertEquals("\t", s);
  }
}