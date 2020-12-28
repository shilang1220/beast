package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.test.JavaSparkTest;

public class FeatureReaderTest extends JavaSparkTest {

  public void testGetFileExtension() {
    String expectedExtension = ".geojson";
    String actualExtension = FeatureReader.getFileExtension("geojson");
    assertEquals(expectedExtension, actualExtension);
  }

}