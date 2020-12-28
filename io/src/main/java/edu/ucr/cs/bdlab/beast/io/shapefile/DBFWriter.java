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
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A record writer that writes features into DBF file format. It should be accompanied with a {@link ShapefileGeometryWriter}
 * to write a correct shapefile.
 */
public class DBFWriter extends RecordWriter<Object, IFeature> {

  /**Path to the desired DBF file*/
  private Path dbfPath;

  /**Configuration of the job*/
  private Configuration conf;

  /**A temporary file for writing the records until all records have been written*/
  protected File tempDbfFile;

  /**The output stream that writes to the temporary DBF file*/
  protected ObjectOutputStream tempDbfOut;

  /**File header is updated while the features are written and is flushed to disk at the very end*/
  protected DBFHeader header;

  /**
   * Initializes the record writer to write to the given DBF file.
   * @param dbfPath the path to the output file
   * @param conf the system configuration
   * @throws IOException if an error happens while initializing the output
   */
  public void initialize(Path dbfPath, Configuration conf) throws IOException {
    this.dbfPath = dbfPath;
    this.conf = conf;

    // We cannot write the final DBF file directly due to unknown header information, e.g., number of records
    // This class first writes a temporary file with the feature data and write the final file upon closure
    tempDbfFile = File.createTempFile(dbfPath.getName(), ".dbf.tmp");
    tempDbfOut = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tempDbfFile)));
    tempDbfFile.deleteOnExit();

    header = new DBFHeader();
  }

  @Override
  public void write(Object key, IFeature value) throws IOException {
    header.numRecords++;
    if (header.fieldDescriptors == null) {
      // First feature, get the header from it
      header.fieldDescriptors = new FieldDescriptor[value.getNumAttributes()];
      for (int iAttr = 0; iAttr < value.getNumAttributes(); iAttr++) {
        FieldDescriptor attr = header.fieldDescriptors[iAttr] = new FieldDescriptor();
        attr.fieldName = new byte[11]; // Field name with a maximum of 11 characters, initially filled with zeros
        if (value.getAttributeName(iAttr) != null) {
          // Crop the name to 11 bytes
          byte[] fullName = value.getAttributeName(iAttr).getBytes();
          System.arraycopy(fullName, 0, attr.fieldName, 0, Math.min(11, fullName.length));
        }
        Object fieldValue = value.getAttributeValue(iAttr);
        switch (value.getAttributeType(iAttr)) {
          case StringType:
            attr.fieldType = DBFConstants.TypeString;
            break;
          case IntegerType:
            attr.fieldType = DBFConstants.TypeNumeric;
            attr.fieldLength = fieldValue == null? 0 : (short) String.valueOf(((Number)fieldValue).intValue()).length();
            break;
          case LongType:
            attr.fieldType = DBFConstants.TypeNumeric;
            attr.fieldLength = fieldValue == null? 0 : (short) String.valueOf(((Number)fieldValue).longValue()).length();
            break;
          case DoubleType:
            attr.fieldType = DBFConstants.TypeDouble;
            attr.fieldLength = 8; // 64-bit floating point
            break;
          case BooleanType:
            // Logical
            attr.fieldType = DBFConstants.TypeBoolean;
            attr.fieldLength = 1; // One byte which is initialize to 0x20 (space) otherwise, 'T' or 'F'
            break;
          case TimestampType:
            // Date time field
            attr.fieldType = DBFConstants.TypeDatetime;
            attr.fieldLength = 8; // 8 bytes - two longs, first for date, second for time.
            // The date is the number of days since  01/01/4713 BC.
            // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
            break;
          default:
            throw new RuntimeException("Unsupported attribute value type: "+value.getAttributeType(iAttr));
        }
      }
    }
    // Write the feature attribute values to the temporary file
    tempDbfOut.writeObject(value);
    // Update field lengths
    for (int iAttr = 0; iAttr < value.getNumAttributes(); iAttr++) {
      Object fieldValue = value.getAttributeValue(iAttr);
      switch (value.getAttributeType(iAttr)) {
        case StringType:
        case IntegerType:
        case LongType:
          header.fieldDescriptors[iAttr].fieldLength = (short) Math.max(header.fieldDescriptors[iAttr].fieldLength,
              String.valueOf(fieldValue).length());
      }
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    tempDbfOut.close();
    LocalDateTime now = LocalDateTime.now();
    header.version = 3;
    header.dateLastUpdatedYY = (short) (now.getYear() - 1900);
    header.dateLastUpdatedMM = (short) now.getMonthValue();
    header.dateLastUpdatedDD = (short) now.getDayOfMonth();

    // Calculate header size and record size
    header.headerSize = 32 /*Main header*/ +
        32 * header.fieldDescriptors.length /*field descriptors*/ +
        1 /*Header record terminator*/;
    header.recordSize = 1; // Terminator
    for (FieldDescriptor field : header.fieldDescriptors)
      header.recordSize += field.fieldLength;

    FileSystem fileSystem = dbfPath.getFileSystem(conf);
    FSDataOutputStream dbfOut = fileSystem.create(dbfPath);

    // Write header
    writeHeader(dbfOut, header);
    // Write records
    // Create a new feature to make sure the geometry in it is not reused outside this class.
    try (ObjectInputStream tempDbfIn = new ObjectInputStream(new BufferedInputStream(new FileInputStream(tempDbfFile)))) {
      for (int i = 0; i < header.numRecords; i++) {
        IFeature f = (IFeature) tempDbfIn.readObject();
        dbfOut.write(DBFReader.ValidRecordMarker);
        writeRecord(dbfOut, f);
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Error reading features back", e);
    }
    // Delete the temp DBF file
    tempDbfFile.delete();
    // Write record terminator
    dbfOut.write(DBFReader.EOFMarker);
    dbfOut.close();
  }

  protected void writeHeader(DataOutputStream out, DBFHeader header) throws IOException {
    out.write(header.version);
    out.write(header.dateLastUpdatedYY);
    out.write(header.dateLastUpdatedMM);
    out.write(header.dateLastUpdatedDD);
    IOUtil.writeIntLittleEndian(out, header.numRecords);
    IOUtil.writeShortLittleEndian(out, (short) header.headerSize);
    IOUtil.writeShortLittleEndian(out, (short) header.recordSize);
    // Skip 16 bytes
    out.writeLong(0);
    out.writeLong(0);
    out.write(0); // No special flags
    out.write(0); // Code page mark
    out.writeShort(0); // Reserved. Filled with zeros
    // Write field descriptors
    int fieldDisplacement = 0;
    for (FieldDescriptor descriptor : header.fieldDescriptors) {
      assert descriptor.fieldName.length == 11;
      out.write(descriptor.fieldName);
      out.write(descriptor.fieldType);
      out.writeInt(fieldDisplacement); // Field displacement in record
      out.write(descriptor.fieldLength);
      out.write(descriptor.decimalCount);
      out.write(0); // Field flags
      out.writeInt(0); // Value of autoincrement Next value
      out.write(0); // Value of autoincrement Step value
      out.writeLong(0); // Reserved (8-bytes)
      fieldDisplacement += descriptor.fieldLength;
    }
    out.write(0x0D); // Header record terminator
  }

  protected void writeRecord(DataOutputStream out, IFeature feature) throws IOException {
    for (int iAttr = 0; iAttr < feature.getNumAttributes(); iAttr++) {
      Object value = feature.getAttributeValue(iAttr);
      if (value == null) {
        // Write a default value that will be parsed back as null
        switch (feature.getAttributeType(iAttr)) {
          case StringType:
            for (int i = 0; i < header.fieldDescriptors[iAttr].fieldLength; i++)
              out.write(0);
            break;
          case IntegerType:
          case LongType:
            for (int i = 0; i < header.fieldDescriptors[iAttr].fieldLength; i++)
              out.write(0x20); // White space
            break;
          case DoubleType:
            IOUtil.writeDoubleLittleEndian(out, Double.NaN);
            break;
          case BooleanType:
            out.write(0x20); // White space
            break;
          case TimestampType:
            IOUtil.writeLongLittleEndian(out, 0);
            break;
          default:
            throw new RuntimeException("Unsupported type " + feature.getAttributeType(iAttr) + " for null values");
        }
      } else {
        byte[] valBytes;
        int diff;
        switch (feature.getAttributeType(iAttr)) {
          case StringType:
            valBytes = ((String) value).getBytes();
            out.write(valBytes);
            diff = header.fieldDescriptors[iAttr].fieldLength - valBytes.length;
            assert diff >= 0 : "Value longer than expected";
            // Append spaces
            while (diff-- > 0)
              out.write(' ');
            break;
          case IntegerType:
          case LongType:
            valBytes = String.valueOf(value).getBytes();
            diff = header.fieldDescriptors[iAttr].fieldLength - valBytes.length;
            assert diff >= 0 : "Value longer than expected";
            // Prepend spaces
            while (diff-- > 0)
              out.write(' ');
            out.write(valBytes);
            break;
          case DoubleType:
            IOUtil.writeDoubleLittleEndian(out, ((Number) value).doubleValue());
            break;
          case BooleanType:
            out.writeByte((Boolean) value ? 'T' : 'F');
            break;
          case TimestampType:
            // two (32-bit) longs, first for date, second for time.
            // The date is the number of days since  01/01/4713 BC.
            // Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
            GregorianCalendar datetime = (GregorianCalendar) value;
            if (datetime.getTimeZone().getRawOffset() != 0) {
              // Convert to UTC time
              ZonedDateTime utctime = ZonedDateTime.ofInstant(datetime.toZonedDateTime().toInstant(), ZoneOffset.ofTotalSeconds(0));
              datetime = GregorianCalendar.from(utctime);
            }

            int datepart = (int) ((datetime.getTimeInMillis() - DBFConstants.DBFEpoch.getTimeInMillis()) / DBFConstants.MillisInOneDay);
            int timepart = (int) (datetime.getTimeInMillis() % DBFConstants.MillisInOneDay);
            assert timepart == datetime.get(Calendar.HOUR_OF_DAY) * 3600000 +
                datetime.get(Calendar.MINUTE) * 60000 +
                datetime.get(Calendar.SECOND) * 1000;
            IOUtil.writeIntLittleEndian(out, datepart);
            IOUtil.writeIntLittleEndian(out, timepart);
            break;
          default:
            throw new RuntimeException("Unsupported type " + feature.getAttributeType(iAttr) + " for null values");
        }
      }
    }
  }
}
