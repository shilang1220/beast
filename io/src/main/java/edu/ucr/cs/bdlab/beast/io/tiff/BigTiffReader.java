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
package edu.ucr.cs.bdlab.beast.io.tiff;

import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads TIFF images.
 */
public class BigTiffReader implements ITiffReader, Closeable {

  public static final short Signature = 43;

  /**
   * 8-byte header of the TIFF file. See specs page 13.
   */
  static class Header {
    static final short LITTLE_ENDIAN = 0x4949;
    static final short BIG_ENDIAN = 0x4D4D;

    /**Byte ordering. Either LITTLE_ENDIAN or BIG_ENDIAN*/
    public short order;

    /** An arbitrary but carefully chosen number (43) that further identifies the file as a BigTIFF file. */
    public short signature;

    /**
     * Bytesize of offsets
     * Always 8 in BigTIFF, it provides a nice way to move to 16byte pointers some day.
     * If there is some other value here, a reader should give up
     */
    public int offsetsSize;

    /**
     * The offset (in bytes) of the first IFD.
     */
    public long offset;

    public Header read(InputStream in) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      int numBytesRead = 0;
      while (numBytesRead < 16) {
        numBytesRead += in.read(buffer.array(), numBytesRead, 16 - numBytesRead);
      }
      order = buffer.getShort();
      assert order == LITTLE_ENDIAN || order == BIG_ENDIAN : String.format("Invalid byte order %d", order);
      buffer.order(order == LITTLE_ENDIAN? ByteOrder.LITTLE_ENDIAN: ByteOrder.BIG_ENDIAN);
      signature = buffer.getShort();
      assert signature == Signature : String.format("Invalid signature %d in TIFF file", signature);
      offsetsSize = buffer.getShort();
      int marker = buffer.getShort();
      assert marker == 0;
      offset = buffer.getLong();
      return this;
    }
  }

  /**The header of this file*/
  protected Header header;

  /**Input stream to the TIFF file*/
  protected FSDataInputStream in;

  /**
   * The list of all directory entries in the file. Each entry in this list contains an array of IFD entries
   * that appear on IFD. According to the TIFF specs on page 16:
   * There may be more than one IFD in a TIFF file. Each IFD defines a subfile. One
   * potential use of subfiles is to describe related images, such as the pages of a facsimile
   * transmission. A Baseline TIFF reader is not required to read any IFDs
   * beyond the first one.
   */
  protected List<BigIFDEntry[]> directoryEntries;

  /**A temporary buffer to read chunks from underlying file*/
  private transient ByteBuffer buffer;

  public void initialize(FSDataInputStream in) throws IOException {
    this.in = in;
    header = new Header().read(in);
    // Read all IFDs and keep them in memory
    directoryEntries = new ArrayList<>();
    buffer = expandBuffer(buffer, 1024);
    long offsetIFD = header.offset;
    while (offsetIFD != 0) {
      in.seek(offsetIFD);
      long numEntries = header.order == Header.LITTLE_ENDIAN?
          IOUtil.readLongLittleEndian(in) : IOUtil.readLongBigEndian(in);
      assert numEntries > 0 : "Found an empty IFD. Each IFD must have at least one entry.";
      assert numEntries < Integer.MAX_VALUE : "Too many IFD entries";
      BigIFDEntry[] entries = new BigIFDEntry[(int) numEntries];
      long sizeIFD = 20 * numEntries + 8;
      // Expand the buffer if necessary to hold the entire table
      buffer = expandBuffer(buffer, (int) sizeIFD);
      buffer.position(0);
      // Read the entire IFD
      in.readFully(buffer.array(), 0, (int) sizeIFD);
      // Set the limit to ensure that we don't mistakenly go beyond the IFD
      buffer.limit((int) sizeIFD);

      // Read all the entries and keep them in memory
      for (int $i = 0; $i < numEntries; $i++)
        entries[$i] = new BigIFDEntry().read(buffer, header.order == BIG_ENDIAN);

      directoryEntries.add(entries);

      // Locate the next IFD (if any)
      offsetIFD = buffer.getLong();
      assert buffer.remaining() == 0 : String.format("The buffer still has %d remaining bytes", buffer.remaining());
    }
  }

  public int getNumLayers() {
    return directoryEntries.size();
  }

  public ByteBuffer readEntry(AbstractIFDEntry entry, ByteBuffer buffer) throws IOException {
    if (entry == null) {
      if (buffer != null) {
        buffer.position(0);
        buffer.limit(0);
      }
      return buffer;
    }
    long size = entry.getCountAsLong() * IFDEntry.TypeSizes[entry.type];
    if (buffer == null || buffer.capacity() < size)
      buffer = expandBuffer(buffer, (int) size);
    buffer.position(0);
    if (size <= 8) {
      // value stored in the offset field
      long val = entry.getOffsetAsLong();
      for (int $i = 0; $i < size; $i++) {
        buffer.put((byte) (val & 0xff));
        val = val >>> 8;
      }
    } else {
      // Value stored in the input file
      in.seek(entry.getOffsetAsLong());
      in.readFully(buffer.array(), 0, (int) size);
    }
    buffer.position(0);
    buffer.limit((int) size);
    return buffer;
  }

  private ByteBuffer expandBuffer(ByteBuffer buffer, int length) {
    if (buffer == null || buffer.capacity() < length) {
      buffer = ByteBuffer.allocate(length);
      buffer.order(header.order == Header.LITTLE_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    }
    buffer.limit(length);
    return buffer;
  }

  /**
   * Returns all the values in the given entry as an integer array. This method has to implemented at the TiffReader
   * level as it might need to read the underlying file.
   * @param entry the entry to read
   * @return the array of values for the given entry
   * @throws IOException if an error happens while reading the file
   */
  public int[] getIntValues(IFDEntry entry) throws IOException {
    int[] values = new int[entry.count];
    buffer = readEntry(entry, buffer);
    switch (entry.type) {
      case IFDEntry.TYPE_BYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xff & buffer.get();
        break;
      case IFDEntry.TYPE_SBYTE:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.get();
        break;
      case IFDEntry.TYPE_SHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = 0xffff & buffer.getShort();
        break;
      case IFDEntry.TYPE_SSHORT:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getShort();
        break;
      case IFDEntry.TYPE_LONG:
        // TODO we should handle unsigned 32-bit integers that are larger than INT_MAX
      case IFDEntry.TYPE_SLONG:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getInt();
        break;
    }
    return values;
  }

  public Raster getLayer(int i) throws IOException {
    return new Raster(this, i);
  }

  @Override
  public AbstractIFDEntry[] getDirectoryEntries(int iLayer) {
    return directoryEntries.get(iLayer);
  }

  @Override
  public void readRawData(long tileOffset, byte[] bytes) throws IOException {
    in.readFully(tileOffset, bytes);
  }

  @Override
  public boolean isLittleEndian() {
    return header.order == LITTLE_ENDIAN;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

}
