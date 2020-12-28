package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

public class DBFReaderTest extends JavaSparkTest {

  public void testSimpleReader() throws IOException {
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource("/usa-major-cities/usa-major-cities.dbf", new File(dbfPath.toString()));
    DBFReader reader = new DBFReader();
    try {
      reader.initialize(dbfPath, new Configuration());
      assertEquals(120, reader.header.numRecords);
      assertEquals(4, reader.header.fieldDescriptors.length);
      int recordCount = 0;
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        assertEquals(4, feature.getNumAttributes());
        assertNotNull(feature.getAs("RuleID"));
        assertEquals(feature.getAttributeValue(3), feature.getAs("RuleID"));
        recordCount++;
      }
      assertEquals(120, recordCount);
    } finally {
      reader.close();
    }
  }

  public void testParseFields() throws IOException {
    String[] dbfFiles = {
        "/usa-major-cities/usa-major-cities.dbf",
        "/usa-major-highways/usa-major-highways.dbf",
        "/simple-with-dates.dbf"
    };
    for (String dbfFile : dbfFiles) {
      Path dbfPath = new Path(scratchPath(), "temp.dbf");
      copyResource(dbfFile, new File(dbfPath.toString()));
      DBFReader reader = new DBFReader();
      try {
        reader.initialize(dbfPath, new Configuration());
        while (reader.nextKeyValue()) {
          IFeature feature = reader.getCurrentValue();
          for (int i = 0; i < feature.getNumAttributes(); i++)
            assertNotNull(feature.getAttributeValue(i));
        }
      } finally {
        reader.close();
        new File(dbfPath.toString()).delete();
      }
    }
  }

  public void testParseDates() throws IOException {
    String dbfFile = "/simple-with-dates.dbf";
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource(dbfFile, new File(dbfPath.toString()));
    DBFReader reader = new DBFReader();
    try {
      reader.initialize(dbfPath, new Configuration());
      assertTrue("Did not find records in the file", reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      GregorianCalendar expected = new GregorianCalendar(DBFConstants.UTC);
      expected.clear();
      expected.set(2018, 12 - 1, 26);
      assertEquals(expected.getTimeInMillis(),
          ((GregorianCalendar)feature.getAttributeValue(1)).getTimeInMillis());
    } finally {
      reader.close();
      new File(dbfPath.toString()).delete();
    }
  }

  public void testImmutableObjects() throws IOException {
    Path dbfPath = new Path(scratchPath(), "temp.dbf");
    copyResource("/usa-major-cities/usa-major-cities.dbf", new File(dbfPath.toString()));
    DBFReader reader = new DBFReader();
    try {
      Configuration conf = new Configuration();
      reader.initialize(dbfPath, conf);
      assertEquals(120, reader.header.numRecords);
      assertEquals(4, reader.header.fieldDescriptors.length);
      int recordCount = 0;
      List<IFeature> features = new ArrayList<IFeature>();
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        features.add(feature);
        assertEquals(4, feature.getNumAttributes());
        assertNotNull(feature.getAs("RuleID"));
        assertEquals(feature.getAttributeValue(3), feature.getAs("RuleID"));
        recordCount++;
      }
      assertEquals(120, recordCount);
      assertNotSame(features.get(0), features.get(1));
    } finally {
      reader.close();
    }
  }

  public void testShouldNotReturnNullOnEmptyStrings() throws IOException {
    DBFReader reader = new DBFReader();
    try {
      DataInputStream in = new DataInputStream(this.getClass().getResourceAsStream("/file_with_empty_numbers.dbf"));
      reader.initialize(in, new Configuration());
      while (reader.nextKeyValue()) {
        IFeature feature = reader.getCurrentValue();
        // No assertions. Just make sure that no errors are raised
      }
    } finally {
      reader.close();
    }
  }

  public void testShouldIgnoreTrailingZerosInStrings() throws IOException {
    try (DBFReader reader = new DBFReader()) {
      DataInputStream in = new DataInputStream(this.getClass().getResourceAsStream("/nullstrings.dbf"));
      reader.initialize(in, new Configuration());
      assertTrue(reader.nextKeyValue());
      IFeature feature = reader.getCurrentValue();
      assertNull(feature.getAttributeValue(1));
      assertTrue(reader.nextKeyValue());
      feature = reader.getCurrentValue();
      assertEquals("Hello", feature.getAttributeValue(1));
    }
  }
}