/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.FieldType;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.GregorianCalendar;

public class DBFWriterTest extends JavaSparkTest {

  public void testSimpleWriter() throws IOException {
    DBFReader dbfReader = new DBFReader();
    DBFWriter dbfWriter = new DBFWriter();
    String[] dbfFileNames = {"usa-major-cities"};
    for (String dbFileName : dbfFileNames) {
      Path dbfPath = new Path(scratchPath(), dbFileName+".dbf");
      Configuration conf = new Configuration();
      // Create a DBF file that is a copy of the existing file
      try {
        DataInputStream dbfInput = new DataInputStream(getClass().getResourceAsStream("/" + dbFileName + "/" + dbFileName + ".dbf"));
        dbfReader.initialize(dbfInput, conf);
        dbfWriter.initialize(dbfPath, conf);
        while (dbfReader.nextKeyValue()) {
          dbfWriter.write(NullWritable.get(), dbfReader.getCurrentValue());
        }
      } finally {
        dbfReader.close();
        dbfWriter.close(null);
      }

      // Read the written file back and make sure it is similar to the original one
      DBFReader reader2 = new DBFReader();
      try {
        DataInputStream dbfInput = new DataInputStream(getClass().getResourceAsStream("/"+dbFileName+"/"+dbFileName+".dbf"));
        dbfReader.initialize(dbfInput, conf);
        reader2.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), conf);
        while (dbfReader.nextKeyValue()) {
          assertTrue("The written file contains too few records", reader2.nextKeyValue());
          assertEquals(dbfReader.getCurrentValue(), reader2.getCurrentValue());
        }
      } finally {
        dbfReader.close();
        reader2.close();
      }
    }
  }

  public void testShouldWriteFeaturesWithoutNames() throws IOException {
    // A sample feature
    Feature f = new Feature(null, null, null, new Object[]{"abc", "def"});

    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "test.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one record
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(null, f);
    } finally {
      dbfWriter.close(null);
    }

    // Read the written file back and make sure it is correct
    DBFReader reader = new DBFReader();
    try {
      reader.initialize(dbfPath, conf);
      int count = 0;
      while (reader.nextKeyValue()) {
        count++;
        assertEquals(reader.getCurrentValue(), f);
      }
      assertEquals(1, count);
    } finally {
      reader.close();
      reader.close();
    }
  }

  public void testWriteAllAttributeTypes() throws IOException, InterruptedException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    GregorianCalendar date = new GregorianCalendar(Feature.UTC());
    date.clear();
    date.set(2019, 9, 9);
    Feature f = new Feature(
        null,
        new String[]{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"},
        null,
        new Object[]{Boolean.TRUE, 123, (byte) 12, (short) 13, (long) 6546543, 12.25f, 12.655847, "value9", date}
    );
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(null, f);
    } finally {
      try {
        dbfWriter.close(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    DBFReader dbfReader = new DBFReader();
    try {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), conf);
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      assertEquals("The features should be equal", f, f2);
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    } finally {
      dbfReader.close();
    }
  }

  public void testWriteNegativeIntegers() throws IOException, InterruptedException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    Feature f = new Feature(null, new String[]{"key1", "key2", "key3"},
        null, new Object[]{-123, Integer.MIN_VALUE, Long.MAX_VALUE});
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(null, f);
    } finally {
      try {
        dbfWriter.close(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    DBFReader dbfReader = new DBFReader();
    try {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), conf);
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      assertEquals("The features should be equal", f.toString(), f2.toString());
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    } finally {
      dbfReader.close();
    }
  }

  public void testWriteNullAttributeTypes() throws IOException, InterruptedException {
    DBFWriter dbfWriter = new DBFWriter();
    Path dbfPath = new Path(scratchPath(), "output.dbf");
    Configuration conf = new Configuration();
    // Create a DBF file with one feature that has all the possible types
    Feature f = new Feature(null,
        new String[] {"key1", "key2", "key3", "key4", "key5", "key6"},
        new FieldType[]{
            FieldType.BooleanType, FieldType.IntegerType, FieldType.LongType,
            FieldType.DoubleType, FieldType.StringType, FieldType.TimestampType
        }, null
    );
    try {
      dbfWriter.initialize(dbfPath, conf);
      dbfWriter.write(null, f);
    } finally {
      try {
        dbfWriter.close(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Read the written file back and make sure it is similar to the original one
    DBFReader dbfReader = new DBFReader();
    try {
      dbfReader.initialize(new DataInputStream(new FileInputStream(dbfPath.toString())), conf);
      assertTrue("The output should have one record", dbfReader.nextKeyValue());
      IFeature f2 = dbfReader.getCurrentValue();
      for (int i = 0; i < 6; i++)
        assertNull("Attribute " + i + " should be null", f2.getAttributeValue(i));
      assertEquals(FieldType.BooleanType, f2.getAttributeType(0));
      assertEquals(FieldType.IntegerType, f2.getAttributeType(1));
      assertEquals(FieldType.IntegerType, f2.getAttributeType(2));
      assertEquals(FieldType.DoubleType, f2.getAttributeType(3));
      assertEquals(FieldType.StringType, f2.getAttributeType(4));
      assertEquals(FieldType.TimestampType, f2.getAttributeType(5));
      assertFalse("The output should not have more than one record", dbfReader.nextKeyValue());
    } finally {
      try {
        dbfWriter.close(null);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}