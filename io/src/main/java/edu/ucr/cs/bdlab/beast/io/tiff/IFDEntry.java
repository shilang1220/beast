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

import java.nio.ByteBuffer;

/**
 * A directory entry in the TIFF file. See specs page 14.
 */
public class IFDEntry extends AbstractIFDEntry {

  /**The number of values, Count of the indicated Type*/
  public int count;

  /**
   * The Value Offset, the file offset (in bytes) of the Value for the field.
   * The Value is expected to begin on a word boundary; the corresponding
   * Value Offset will thus be an even number. This file offset may
   * point anywhere in the file, even after the image data.
   */
  public int offset;

  public IFDEntry read(ByteBuffer buffer, boolean bigEndian) {
    this.tag = buffer.getShort();
    this.type = buffer.getShort();
    this.count = buffer.getInt();
    this.offset = buffer.getInt();
    if (bigEndian && getLength() <= 4) {
      int newValue;
      // Correct the value for big endian depending on the type if the actual value is stored in the offset field
      switch (TypeSizes[type]) {
        case 1:
          newValue = this.offset >>> 24;
          newValue |= (this.offset >>> 8) & 0xff00;
          newValue |= (this.offset << 8) & 0xff0000;
          newValue |= (this.offset << 24) & 0xff000000;
          break;
        case 2:
          newValue = (this.offset >>> 16) & 0xffff;
          newValue |= (this.offset << 16) & 0xffff0000;
          break;
        case 4:
          newValue = this.offset;
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type %d of length %d", type, TypeSizes[type]));
      }
      this.offset = newValue;
    }
    return this;
  }

  @Override
  public int getCountAsInt() {
    return count;
  }

  @Override
  public long getCountAsLong() {
    return count;
  }

  @Override
  public int getOffsetAsInt() {
    return offset;
  }

  @Override
  public long getOffsetAsLong() {
    return offset;
  }
}
