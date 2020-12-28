package ${groupId};

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.spark.api.java.JavaRDD;

import java.io.File;

public class BeastJavaTest extends JavaSparkTest {
  public void testComputeTwoRoundsPoints() {
    // Make a copy of a file from resources
    File input = makeFileCopy("/input.txt");

    // Read the file
    JavaRDD<IFeature> polygons = SpatialReader.readInput(javaSparkContext(),
        new BeastOptions().set(CSVFeatureReader.FieldSeparator, ","), input.getPath(), "point");

    // Add your tests here
    assertEquals(2, polygons.count());
  }
}