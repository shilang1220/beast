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
package edu.ucr.cs.bdlab.beast.util;

public class MathUtil {

  /**
   * Computes the number of significant bits in the given integer. This also represents the minimum number of bits
   * needed to represent this number.
   * Special case, if {@code x} is 0, the return value is 1.
   * @param x any integer
   * @return number of significant bits
   */
  public static int numberOfSignificantBits(int x) {
    if (x == 0)
      return 1;
    int numBits = 0;
    if ((x & 0xffff0000) != 0) {
      numBits += 16;
      x >>>= 16;
    }
    if ((x & 0xff00) != 0) {
      numBits += 8;
      x >>>= 8;
    }
    if ((x & 0xf0) != 0) {
      numBits += 4;
      x >>>= 4;
    }
    if ((x & 0xc) != 0) {
      numBits += 2;
      x >>>= 2;
    }
    if ((x & 0x2) != 0) {
      numBits += 1;
      x >>>= 1;
    }
    if (x != 0)
      numBits++;
    return numBits;
  }

  /**
   * Retrieves the specified number of bits from the given array of bytes.
   * @param data the sequence of all bytes
   * @param position the bit position to start retrieving
   * @param length the number of bits to retrieve
   * @return the retrieved bits stored in an integer
   */
  public static long getBits(byte[] data, int position, int length) {
    long value = 0;
    int start = position / 8;
    int end = (position + length - 1) / 8;
    while (start <= end)
      value = (value << 8) | (data[start++] & 0xff);
    value = value >>> (7 - (position + length - 1) % 8);
    value = value & (0xffffffffffffffffL >>> (64 - length));
    return value;
  }

  /**
   * Sets the given range of positions to the lowest bits of the given value.
   * @param data the array of bytes to modify
   * @param position the first bit to change starts with position 0 in the array and treating the most significant
   *                 bit as the first position in each byte
   * @param length total number of bits to change and to take from the given value
   * @param value the lowest significant bits of the given value will be used to set bits in the data array
   */
  public static void setBits(byte[] data, int position, int length, int value) {
    // Mask left marks the bits that will change on the lowest byte position
    byte maskLeft = (byte) (0xff >> (position % 8));
    // Mask right marks the bits that will change on the highest byte position
    int lastBitExclusive = position + length;
    byte maskRight = (byte) (0xff00 >> (lastBitExclusive % 8));
    int firstByteToChange = position / 8;
    int lastByteToChange = lastBitExclusive / 8;
    if (firstByteToChange == lastByteToChange) {
      // Special case of partially changing one byte
      int mask = maskLeft & maskRight;
      data[firstByteToChange] &= ~mask;
      int lshift = 7 - (lastBitExclusive - 1) % 8;
      data[firstByteToChange] |= (value << lshift) & mask;
      return;
    }
    // General case where we change more than one byte
    // Change the left-most byte
    int rshift = length - (8 - position % 8);
    if (maskLeft != 0xff) {
      data[firstByteToChange] &= ~maskLeft;
      data[firstByteToChange] |= (value >> rshift) & maskLeft;
      firstByteToChange++;
      rshift -= 8;
    }
    // Change all bytes in between (completely changed)
    while (firstByteToChange < lastByteToChange) {
      data[firstByteToChange] = (byte) (value >> rshift);
      firstByteToChange++;
      rshift -= 8;
    }
    // Change the right most byte
    if (maskRight != 0) {
      data[lastByteToChange] &= ~maskRight;
      int lshift = 7 - (lastBitExclusive - 1) % 8;
      data[lastByteToChange] |= (value << lshift) & maskRight;
    }
  }

  public static int nextPowerOfTwo(int i) {
    return Integer.highestOneBit(i) << 1;
  }

  /**
   * Integer floor of base to the log 2. For the special case when the input is zero, this function returns -1.
   * This function is not defined for negative values.
   * @param i the value to compute
   * @return &lfloor; log<sub>2</sub>(i)&rfloor;
   */
  public static int log2(int i) {
    return 32 - Integer.numberOfLeadingZeros(i) - 1;
  }

  /**
   * Long integer floor of base to the log 2. For the special case when the input is zero, this function returns -1.
   * This function is not defined for negative values.
   * @param i the value to compute
   * @return &lfloor; log<sub>2</sub>(i)&rfloor;
   */
  public static int log2(long i) {
    return 64 - Long.numberOfLeadingZeros(i) - 1;
  }
}
