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

import edu.ucr.cs.bdlab.beast.util.MathUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * A raster layer from the TIFF file.
 */
public class Raster {
  private static final Log LOG = LogFactory.getLog(Raster.class);

  /**
   * No compression, but pack data into bytes as tightly as possible, leaving no unused
   * bits (except at the end of a row). The component values are stored as an array of
   * type BYTE. Each scan line (row) is padded to the next BYTE boundary.
   */
  public static final int COMPRESSION_NONE = 1;

  /**1-Dimensional Modified Huffman run length encoding. See Section 10 in the tiff6 specs*/
  public static final int COMPRESSION_CCITT_HUFFMAN = 2;

  /**LZW compression scheme*/
  public static final int COMPRESSION_LZW = 5;

  /**JPEG compression*/
  public static final int COMPRESSION_JPEG = 6;

  /**Deflate compression*/
  public static final int COMPRESSION_DEFLATE = 8;

  /**The associated TIFF file*/
  protected final ITiffReader reader;

  /**
   * A predictor is a mathematical operator that is applied to the image data before an encoding scheme is applied.
   * 1 = No prediction scheme used before coding.
   * 2 = Horizontal differencing.
   */
  protected final int predictor;

  /**IDF entries associated with this layer in the TIFF file*/
  protected AbstractIFDEntry[] entries;

  /**Width of the raster. Number of pixels per row.*/
  protected int width;

  /**Height of the raster. Number of rows (scan lines).*/
  protected int height;

  /**
   * Width of each tile in pixels. If the file is stored in a strip representation, tileWidth = width
   */
  protected int tileWidth;

  /**
   * Height of each tile in pixels. If the file is stored in a strip representation, tileHeight = rowsPerStrip
   */
  protected int tileHeight;

  /**
   * Offsets of tiles if the file is stored in a tile format; or offsets of strips if strip format
   */
  protected long[] tileOffsets;

  /**
   * Sizes of tiles in number of bytes (number of compressed bytes if compressed); if the file is stored in a strip
   * representation, this will be the strip lengths.
   */
  protected int[] tileByteCounts;

  /**Index of the tile that is currently loaded or -1 if not tile is currently loaded.*/
  protected int currentTileIndex = -1;

  /**Number of bits for each sample. A sample is like a component in the pixel.*/
  protected int[] bitsPerSample;

  /**The sum of the entries in the field {@link #bitsPerSample}*/
  protected transient int bitsPerPixel;

  /** A reusable buffer for reading contents of IFD entries*/
  protected transient ByteBuffer buffer;

  /**
   * This field specifies how to interpret each data sample in a pixel. Possible values are:
   * 1 = unsigned integer data
   * 2 = two's complement signed integer data
   * 3 = IEEE floating point data [IEEE]
   * 4 = undefined data format
   */
  protected int[] sampleFormats;

  public static final int SAMPLE_UNSIGNED_INT = 1;
  public static final int SAMPLE_SIGNED_INT = 2;
  public static final int SAMPLE_IEEE_FLOAT = 3;
  public static final int SAMPLE_UNDEFINED = 4;

  /**The decoded (decompressed) data of the current strip.*/
  protected ByteBuffer currentTileData;

  /**
   * How the components of each pixel are stored.
   * 1 = {@link #ChunkyFormat}
   * 2 = {@link #PlanarFormat}
   */
  protected int planarConfiguration;

  /**
   * 1 = Chunky format. The component values for each pixel are stored contiguously.
   * The order of the components within the pixel is specified by
   * PhotometricInterpretation. For example, for RGB data, the data is stored as RGBRGBRGB...
   */
  public static final int ChunkyFormat = 1;

  /**
   * 2 = Planar format. The components are stored in separate "component planes." The
   * values in StripOffsets and StripByteCounts are then arranged as a 2-dimensional
   * array, with SamplesPerPixel rows and StripsPerImage columns. (All of the columns for row 0 are stored first,
   * followed by the columns of row 1, and so on.)
   * PhotometricInterpretation describes the type of data stored in each component plane.
   * For example, RGB data is stored with the Red components in one component plane, the Green in another,
   * and the Blue in another.
   */
  public static final int PlanarFormat = 2;

  /**
   * Initializes a raster layer for the given TIFF file and the given layer number in it.
   * @param reader the TIFF reader of the file
   * @param iLayer the index of the layer to read
   * @throws IOException if an error happens while reading the file
   */
  public Raster(ITiffReader reader, int iLayer) throws IOException {
    this.entries = reader.getDirectoryEntries(iLayer);
    this.reader = reader;
    this.width = getEntry(IFDEntry.TAG_IMAGE_WIDTH).getOffsetAsInt();
    this.height = getEntry(IFDEntry.TAG_IMAGE_LENGTH).getOffsetAsInt();
    // Retrieve the tiling information depending on the representation type
    AbstractIFDEntry rowsPerStrip = getEntry(IFDEntry.TAG_ROWS_PER_STRIP);
    if (rowsPerStrip != null) {
      // File stored in stripped format
      this.tileWidth = this.width;
      this.tileHeight = rowsPerStrip.getOffsetAsInt();
      int numStrips = getNumTilesY();
      // Retrieve offsets of strips
      AbstractIFDEntry ifdStripOffsets = getEntry(IFDEntry.TAG_STRIP_OFFSETS);
      assert ifdStripOffsets.getCountAsInt() == numStrips;
      this.tileOffsets = getLongValues(ifdStripOffsets);
      // Retrieve lengths of strips
      AbstractIFDEntry ifdStripLengths = getEntry(IFDEntry.TAG_STRIP_BYTE_COUNTS);
      assert ifdStripLengths.getCountAsInt() == numStrips;
      this.tileByteCounts = getIntValues(ifdStripLengths);
    } else {
      // File stored in grid format
      this.tileWidth = getEntry(IFDEntry.TAG_TILE_WIDTH).getOffsetAsInt();
      this.tileHeight = getEntry(IFDEntry.TAG_TILE_LENGTH).getOffsetAsInt();
      int numTiles = getNumTilesX() * getNumTilesY();
      // Retrieve tile offsets
      AbstractIFDEntry ifdTileOffsets = getEntry(IFDEntry.TAG_TILE_OFFSETS);
      assert ifdTileOffsets.getCountAsInt() == numTiles;
      this.tileOffsets = getLongValues(ifdTileOffsets);
      // Retrieve tile lengths (sizes in bytes)
      AbstractIFDEntry ifdTileByteCounts = getEntry(IFDEntry.TAG_TILE_BYTE_COUNTS);
      assert ifdTileByteCounts.getCountAsInt() == numTiles;
      this.tileByteCounts = getIntValues(ifdTileByteCounts);
    }
    int samplesPerPixel = getEntry(IFDEntry.TAG_SAMPLES_PER_PIXEL).getOffsetAsInt();
    this.bitsPerSample = getIntValues(getEntry(IFDEntry.TAG_BITS_PER_SAMPLE));
    AbstractIFDEntry sampleFormatEntry = getEntry(IFDEntry.TAG_SAMPLE_FORMAT);
    if (sampleFormatEntry != null) {
      this.sampleFormats = getIntValues(sampleFormatEntry);
    } else {
      // Use default values
      this.sampleFormats = new int[samplesPerPixel];
      Arrays.fill(this.sampleFormats, 1);
    }
    assert this.bitsPerSample.length == this.sampleFormats.length;
    assert this.bitsPerSample.length == samplesPerPixel;
    for (int x : bitsPerSample)
      bitsPerPixel += x;
    this.planarConfiguration = getEntry(IFDEntry.TAG_PLANAR_CONFIGURATION).getOffsetAsInt();
    AbstractIFDEntry predictorEntry = getEntry(IFDEntry.TAG_PREDICTOR);
    this.predictor = predictorEntry == null ? 1 : predictorEntry.getOffsetAsInt();
  }

  /**
   * Width of the raster layer in pixels
   * @return the width of the raster in pixels
   */
  public int getWidth() {
    return width;
  }

  /**
   * Height of the raster layer in pixels.
   * @return the height of the raster in pixels
   */
  public int getHeight() {
    return height;
  }

  /**
   * Number of tiles along the x-axis = &lceil;image width / tile width&rceil;
   * @return the number of tiles along the x-axis
   */
  public int getNumTilesX() {
    return (width + tileWidth - 1) / tileWidth;
  }

  /**
   * Number of tiles along the y-axis = &lceil; image height / tile height &rceil;
   * @return the number of tiles along y-axis
   */
  public int getNumTilesY() {
    return (height + tileHeight - 1) / tileHeight;
  }

  /**
   * Returns the value of the pixel at column i and row j as an integer. If it has more than one component,
   * they are concatenated together into one integer. If the components cannot fit into one long integer
   * (i.e., more than 64 bits), an exception is thrown. The components are ordered so that the first component is stored
   * in the highest order part of the returned value.
   * @param iPixel column-coordinate of the pixel
   * @param jPixel row-coordinate of the pixel
   * @return the pixel value as long
   * @throws IOException if an error happens while reading the file
   */
  public long getPixel(int iPixel, int jPixel) throws IOException {
    if (bitsPerPixel > 64)
      throw new RuntimeException(String.format("The pixel value is too big to fit in a 64-bit integer (%d > 64)", bitsPerPixel));
    loadTileAtPixel(iPixel, jPixel);
    long val = 0;
    int pixelOffset = (jPixel % tileHeight) * tileWidth + (iPixel % tileWidth);
    switch (planarConfiguration) {
      case ChunkyFormat:
        int bitOffset = pixelOffset * bitsPerPixel;
        byte[] dataBytes = currentTileData.array();
        for (int iSample = 0; iSample < bitsPerSample.length; iSample++) {
          long sampleValue = MathUtil.getBits(dataBytes, bitOffset, bitsPerSample[iSample]);
          if (bitsPerSample[iSample] == 16)
            sampleValue = reverseValue(sampleValue, 16);
          val = (val << bitsPerSample[iSample]) | sampleValue;
          bitOffset += bitsPerSample[iSample];
        }
        break;
      case PlanarFormat:
        throw new RuntimeException("Planar format is not yet supported");
    }
    return val;
  }

  /**
   * Loads the data of the tile that contains the given pixel
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @throws IOException if an error happens while reading the file
   */
  public void loadTileAtPixel(int iPixel, int jPixel) throws IOException {
    int iTile = iPixel / tileWidth;
    int jTile = jPixel / tileHeight;
    int requiredStripIndex = jTile * getNumTilesX() + iTile;
//    if(requiredStripIndex==89662)
//      throw new RuntimeException(iPixel + "," + jPixel + "," + tileWidth + "," + tileHeight + "," + iTile + "," + jTile);
    if (requiredStripIndex != currentTileIndex)
      readTileData(requiredStripIndex);
  }

  /**
   * Returns the value of the given pixel and band as an integer
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample which band (or sample) to read for the given pixel
   * @return the sample value as integer
   * @throws IOException if an error happens while reading the file
   */
  public int getSampleValueAsInt(int iPixel, int jPixel, int iSample) throws IOException {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case SAMPLE_IEEE_FLOAT:
        return Math.round(Float.intBitsToFloat((int) val));
      case SAMPLE_SIGNED_INT:
        //diff return value
        return (short) val;
      case SAMPLE_UNSIGNED_INT:
      case SAMPLE_UNDEFINED:
      default:
        return (int) val;
    }
  }

  /**
   * Returns the value of the given pixel and band as an double-precision floating point value
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample as float
   * @throws IOException if an error happens while reading the file
   */
  protected float getSampleValueAsFloat(int iPixel, int jPixel, int iSample) throws IOException {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case SAMPLE_IEEE_FLOAT:
        return Float.intBitsToFloat((int) val);
      case SAMPLE_SIGNED_INT:
        return (short)val;
      case SAMPLE_UNSIGNED_INT:
      case SAMPLE_UNDEFINED:
      default:
        return (float) val;
    }
  }

  /**
   * Get the sample value bits as an integer without interpreting the sample format.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample at the given pixel
   * @throws IOException if an error happens while reading the file
   */
  public long getRawSampleValue(int iPixel, int jPixel, int iSample) throws IOException {
    loadTileAtPixel(iPixel, jPixel);
    long val = 0;
    int pixelOffset = (jPixel % tileHeight) * tileWidth + (iPixel % tileWidth);
    switch (planarConfiguration) {
      case ChunkyFormat:
        int bitOffset = pixelOffset * bitsPerPixel;
        int i = 0;
        while (i < iSample)
          bitOffset += bitsPerSample[i++];
        val = MathUtil.getBits(currentTileData.array(), bitOffset, bitsPerSample[iSample]);
        if (reader.isLittleEndian())
          val = reverseValue(val, bitsPerSample[iSample]);
        break;
      case PlanarFormat:
        throw new RuntimeException("Planar format is not yet supported");
    }
    return val;
  }

  /**
   * Reverse a value (Little Endian &lt; = &gt; Big Endian)
   * @param value the value to reverse
   * @param numBits the number of bits of the given value. Has to be a multiple of 8
   * @return the reversed value.
   */
  protected static long reverseValue(long value, int numBits) {
    if (numBits == 64) {
      long valReversed = value >>> 56;
      valReversed |= (value >>> 40) & 0xff00;
      valReversed |= (value >>> 24) & 0xff0000;
      valReversed |= (value >>> 8) & 0xff000000L;
      valReversed |= (value << 8) & 0xff00000000L;
      valReversed |= (value << 24) & 0xff0000000000L;
      valReversed |= (value << 40) & 0xff000000000000L;
      valReversed |= (value << 56) & 0xff00000000000000L;
      value = valReversed;
    } else if (numBits == 48) {
      long valReversed = value >>> 40;
      valReversed |= (value >>> 24) & 0xff00;
      valReversed |= (value >>> 8) & 0xff0000;
      valReversed |= (value << 8) & 0xff000000L;
      valReversed |= (value << 24) & 0xff00000000L;
      valReversed |= (value << 40) & 0xff0000000000L;
      value = valReversed;
    } else if (numBits == 32) {
      long valReversed = value >>> 24;
      valReversed |= (value >>> 8) & 0xff00;
      valReversed |= (value << 8) & 0xff0000;
      valReversed |= (value << 24) & 0xff000000L;
      value = valReversed;
    } else if (numBits == 16) {
      long valReversed = value >>> 8;
      valReversed |= (value << 8) & 0xff00;
      value = valReversed;
    } else if (numBits == 8) {
      // Do nothing
    } else {
      throw new RuntimeException("Unsupported number of bits "+numBits);
    }
    return value;
  }

  /**
   * Loads the data of the given tile (or strip) in the this raster.
   * If the data is compressed, this function decompresses it. The loaded data is stored in the
   * {@link #currentTileData} field. If the given tile is already loaded, this function does nothing.
   * The field {@link #currentTileIndex} is updated to point to the given tile index.
   * on the fly.
   * @param desiredTileIndex the index of the tile to read
   * @throws IOException if an error happens while reading the file
   */
  protected void readTileData(int desiredTileIndex) throws IOException {
    if (this.currentTileIndex == desiredTileIndex)
      return;
    this.currentTileIndex = desiredTileIndex;
    byte[] rawData = new byte[tileByteCounts[desiredTileIndex]];
    reader.readRawData(tileOffsets[desiredTileIndex], rawData);
    int compressionScheme = getEntry(IFDEntry.TAG_COMPRESSION).getOffsetAsInt();
    byte[] decompressedData;
    switch (compressionScheme) {
      case COMPRESSION_LZW:
          decompressedData = LZWDecoder.decode(rawData);
          currentTileData = ByteBuffer.wrap(decompressedData);
        break;
      case COMPRESSION_NONE:
        currentTileData = ByteBuffer.wrap(rawData);
        break;
      case COMPRESSION_DEFLATE:
        try {
          Inflater inflater = new Inflater();
          inflater.setInput(rawData);
          decompressedData = new byte[getTileWidth() * getTileHeight() * bitsPerPixel / 8];
          int decompressionLength = inflater.inflate(decompressedData);
          assert decompressionLength == decompressedData.length :
                  String.format("Mismatching length between. Decompressed length %d != expected length %d",
                          decompressionLength, decompressedData.length);
          currentTileData = ByteBuffer.wrap(decompressedData);
        } catch (DataFormatException e) {
          throw new IOException("Error decompressing file", e);
        }
        break;
      default:
        throw new RuntimeException(String.format("Unsupported compression scheme %d", compressionScheme));
    }
    if (predictor == 2) {
      // Apply the differencing algorithm
      // Special case when all components are 8-bits which make the differencing simpler
      int minBitsPerSample = bitsPerSample[0];
      int maxBitsPerSample = bitsPerSample[0];
      for (int iSample = 1; iSample < getNumSamples(); iSample++) {
        minBitsPerSample = Math.min(minBitsPerSample, bitsPerSample[iSample]);
        maxBitsPerSample = Math.max(maxBitsPerSample, bitsPerSample[iSample]);
      }
      if (minBitsPerSample == 8 && maxBitsPerSample == 8) {
        // This is the easiest case to handle with an efficient algorithm
        byte[] data = currentTileData.array();
        int numSamples = getNumSamples();
        for (int jPixel = 0; jPixel < tileHeight; jPixel++) {
          int offset = (jPixel * tileWidth + 1) * numSamples;
          int endOffset = ((jPixel + 1) * tileWidth) * numSamples;
          while (offset < endOffset) {
            data[offset] += data[offset - numSamples];
            offset++;
          }
        }
      } else if (minBitsPerSample == 16 && maxBitsPerSample == 16) {
        // Values are short integers
        byte[] data = currentTileData.array();
        int numSamples = getNumSamples();
        short[] previousPixel = new short[numSamples];
        for (int jPixel = 0; jPixel < tileHeight; jPixel++) {
          int offset = (jPixel * tileWidth) * numSamples * 2;
          int endOffset = ((jPixel + 1) * tileWidth) * numSamples * 2;
          for (int iSample = 0; iSample < numSamples; iSample++) {
            previousPixel[iSample] = (short) ((data[offset] & 0xff) | ((data[offset+1] & 0xff) << 8));
            offset += 2;
          }
          while (offset < endOffset) {
            for (int iSample = 0; iSample < numSamples; iSample++) {
              short diff = (short) ((data[offset] & 0xff) | ((data[offset+1] & 0xff) << 8));
              previousPixel[iSample] += diff;
              data[offset] = (byte) previousPixel[iSample];
              data[offset+1] = (byte) (previousPixel[iSample] >> 8);
              offset += 2;
            }
          }
        }
      } else {
        if (planarConfiguration == PlanarFormat)
          throw new RuntimeException("Does not yet support PlanarFormat");
        // General case could be less efficient
        byte[] data = currentTileData.array();
        int numSamples = getNumSamples();
        int[] previousPixel = new int[numSamples];
        for (int jPixel = 0; jPixel < tileHeight; jPixel++) {
          // Offset is in bits
          int offset = (jPixel * tileWidth) * bitsPerPixel;
          int endOffset = ((jPixel + 1) * tileWidth) * bitsPerPixel;
          // Read first pixel (reference pixel)
          for (int iSample = 0; iSample < numSamples; iSample++) {
            previousPixel[iSample] = (int) MathUtil.getBits(data, offset, bitsPerSample[iSample]);
            offset += bitsPerSample[iSample];
          }
          while (offset < endOffset) {
            for (int iSample = 0; iSample < numSamples; iSample++) {
              int diffValue = (int) MathUtil.getBits(data, offset, bitsPerSample[iSample]);
              int correctValue = previousPixel[iSample] + diffValue;
              MathUtil.setBits(data, offset, bitsPerSample[iSample], correctValue);
              previousPixel[iSample] = correctValue;
              offset += bitsPerSample[iSample];
            }
          }
        }
      }
    }

  }

  public final int getTileWidth() {
    return tileWidth;
  }

  public final int getTileHeight() {
    return tileHeight;
  }

  public final int getNumSamples() {
    return bitsPerSample.length;
  }

  /**
   * Returns all the sample values at the given pixel location as integer. If these samples are stored in other formats,
   * e.g., byte or short, they are converted to integer with the same value. If the samples are represented as
   * floating-point numbers, they are rounded to the nearest integer.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param value the array of values to output to
   * @throws IOException if an error happens while reading the pixel value
   */
  public void getPixelSamplesAsInt(int iPixel, int jPixel, int[] value) throws IOException {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsInt(iPixel, jPixel, iSample);
  }

  public void getPixelSamplesAsFloat(int iPixel, int jPixel, float[] value) throws IOException {
    assert value.length == getNumSamples();
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsFloat(iPixel, jPixel, iSample);
  }

  /**
   * Returns the list of IFD entries for this raster.
   * @return the list of all IFD Entries
   */
  public AbstractIFDEntry[] getEntries() {
    return entries;
  }

  /**
   * Returns the IFD Entry for the given tag and associated with this raster.
   * @param tag the tag of the entry to read
   * @return the IFDEntry with the given tag.
   */
  public AbstractIFDEntry getEntry(short tag) {
    // TODO if the list is long, use binary search as the entries are already sorted by tag
    for (AbstractIFDEntry entry : entries) {
      if (entry.tag == tag)
        return entry;
    }
    return null;
  }

  public static final Map<Short, String> TagNames = new HashMap<>();
  static {
    TagNames.put(IFDEntry.TAG_NEW_SUBFILE_TYPE, "New Subfile Type");
    TagNames.put(IFDEntry.TAG_IMAGE_WIDTH, "Image Width");
    TagNames.put(IFDEntry.TAG_IMAGE_LENGTH, "Image Length");
    TagNames.put(IFDEntry.TAG_BITS_PER_SAMPLE, "Bits per sample");
    TagNames.put(IFDEntry.TAG_COMPRESSION, "Compression");
    TagNames.put(IFDEntry.TAG_PHOTOMETRIC_INTERPRETATION, "Photometric Interpretation");
    TagNames.put(IFDEntry.TAG_DOCUMENT_NAME, "Document Name");
    TagNames.put(IFDEntry.TAG_STRIP_OFFSETS, "Strip Offsets");
    TagNames.put(IFDEntry.TAG_ORIENTATION, "Orientation");
    TagNames.put(IFDEntry.TAG_SAMPLES_PER_PIXEL, "Samples per Pixel");
    TagNames.put(IFDEntry.TAG_ROWS_PER_STRIP, "Rows pre Strip");
    TagNames.put(IFDEntry.TAG_STRIP_BYTE_COUNTS, "Strip Byte Counts");
    TagNames.put(IFDEntry.TAG_XRESOLUTION, "X Resolution");
    TagNames.put(IFDEntry.TAG_YRESOLUTION, "Y Resolution");
    TagNames.put(IFDEntry.TAG_PLANAR_CONFIGURATION, "Planar Configuration");
    TagNames.put(IFDEntry.TAG_RESOLUTION_UNIT, "Resolution Unit");
    TagNames.put(IFDEntry.TAG_PAGE_NUMBER, "Page Number");
    TagNames.put(IFDEntry.TAG_PREDICTOR, "Predictor");
    TagNames.put(IFDEntry.TAG_COLOR_MAP, "Color Map");
    TagNames.put(IFDEntry.TAG_TILE_WIDTH, "Tile Width");
    TagNames.put(IFDEntry.TAG_TILE_LENGTH, "Tile Length");
    TagNames.put(IFDEntry.TAG_TILE_OFFSETS, "Tile Offsets");
    TagNames.put(IFDEntry.TAG_TILE_BYTE_COUNTS, "Tile Byte Counts");
    TagNames.put(IFDEntry.TAG_EXTRA_SAMPLES, "Extra Samples");
    TagNames.put(IFDEntry.TAG_SAMPLE_FORMAT, "Sample Format");
    TagNames.put(IFDEntry.TAG_FILL_ORDER, "Fill Order");
    TagNames.put(IFDEntry.TAG_WHITE_POINT, "White Point");
    TagNames.put(IFDEntry.TAG_PRIMARY_CHROMATICITIES, "Primary Chromaticities");
    TagNames.put(IFDEntry.TAG_JPEG_TABLES, "JPEG Tables");
    TagNames.put(IFDEntry.TAG_REFERENCE_BLACK_WHITE, "Reference black white");
  }

  /**
   * Write all entry data to the given print stream; used for debugging purposes.
   * @param out the print stream to dump the entries to, e.g., System.out
   * @throws IOException if an error happens while writing to the PrintStream
   */
  public void dumpEntries(PrintStream out) throws IOException {
    ByteBuffer buffer = null;
    for (AbstractIFDEntry entry : entries) {
      buffer = reader.readEntry(entry, buffer);
      out.printf("#%d ", entry.tag & 0xffff);
      String tagName = TagNames.get(entry.tag);
      out.print(tagName != null? tagName : "Unknown Tag");
      out.print(" - ");
      if (entry.getCountAsInt() > 1)
        out.print("[");
      switch (entry.type) {
        case IFDEntry.TYPE_BYTE:
        default:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.get() & 0xff);
          }
          break;
        case IFDEntry.TYPE_SBYTE:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.get());
          }
          break;
        case IFDEntry.TYPE_SHORT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getShort() & 0xffff);
          }
          break;
        case IFDEntry.TYPE_SSHORT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getShort());
          }
          break;
        case IFDEntry.TYPE_LONG:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getInt() & 0xffffffffL);
          }
          break;
        case IFDEntry.TYPE_SLONG:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%d", buffer.getInt());
          }
          break;
        case IFDEntry.TYPE_ASCII:
          out.print('"');
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            char c = (char) buffer.get();
            if (c == 0) {
              out.print('"');
              if ($i < entry.getCountAsInt() - 1) {
                out.print(", \"");
              }
            } else {
              out.print(c);
            }
          }
          break;
        case IFDEntry.TYPE_FLOAT:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%f", Float.intBitsToFloat(buffer.getInt()));
          }
          break;
        case IFDEntry.TYPE_DOUBLE:
          for (int $i = 0; $i < entry.getCountAsInt(); $i++) {
            if ($i > 0)
              out.print(", ");
            out.printf("%f", Double.longBitsToDouble(buffer.getLong()));
          }
          break;
      }
      if (entry.getCountAsInt() > 1)
        out.print("]");
      out.println();
    }
  }

  /**
   * Returns all the values in the given entry as an integer array. This method has to implemented at the TiffReader
   * level as it might need to read the underlying file.
   * @param entry the entry to get its value
   * @return the array of value as int[]
   * @throws IOException if an error happens while reading the file
   */
  public int[] getIntValues(AbstractIFDEntry entry) throws IOException {
    int[] values = new int[entry.getCountAsInt()];
    buffer = reader.readEntry(entry, buffer);
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
      case IFDEntry.TYPE_LONG8:
      case IFDEntry.TYPE_SLONG8:
      case IFDEntry.TYPE_IFD8:
        for (int $i = 0; $i < values.length; $i++) {
          long value = buffer.getLong();
          if (value > Integer.MAX_VALUE)
            throw new RuntimeException("Value too big");
          values[$i] = (int) value;
        }
        break;
      default:
        throw new RuntimeException(String.format("Unsupported type %d", entry.type));
    }
    return values;
  }


  /**
   * Returns all the values in the given entry as an integer array. This method has to implemented at the TiffReader
   * level as it might need to read the underlying file.
   * @param entry the entry to get its value
   * @return the array of values as long[]
   * @throws IOException if an error happens while reading the file
   */
  public long[] getLongValues(AbstractIFDEntry entry) throws IOException {
    long[] values = new long[entry.getCountAsInt()];
    buffer = reader.readEntry(entry, buffer);
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
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = (long)buffer.getInt() & 0xffffffffL;
        break;
        // TODO we should handle unsigned 32-bit integers that are larger than INT_MAX
      case IFDEntry.TYPE_SLONG:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getInt();
        break;
      case IFDEntry.TYPE_LONG8:
      case IFDEntry.TYPE_SLONG8:
      case IFDEntry.TYPE_IFD8:
        for (int $i = 0; $i < values.length; $i++)
          values[$i] = buffer.getLong();
        break;
      default:
        throw new RuntimeException(String.format("Unsupported type %d", entry.type));
    }
    return values;
  }
}
