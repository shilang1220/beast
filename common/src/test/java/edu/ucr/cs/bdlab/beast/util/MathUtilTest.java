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

import junit.framework.TestCase;

public class MathUtilTest extends TestCase {

  public void testNumSignificantBits() {
    assertEquals(5, MathUtil.numberOfSignificantBits(31));
    assertEquals(24, MathUtil.numberOfSignificantBits(16000000));
    assertEquals(1, MathUtil.numberOfSignificantBits(0));
  }

  public void testGetBits() {
    byte[] data = {0x63, (byte) 0x94, 0x5f, 0x0C};
    assertEquals(0x39, MathUtil.getBits(data, 4, 8));
    assertEquals(0x394, MathUtil.getBits(data, 6, 10));
    assertEquals(0xA2F, MathUtil.getBits(data, 11, 12));
    assertEquals(0x63945F0C, MathUtil.getBits(data, 0, 32));
  }

  public void testSetBits() {
    byte[] data = {0, 0, 0, 0};
    MathUtil.setBits(data, 8, 4, 0x9);
    assertEquals((byte) 0x90, data[1]);
    MathUtil.setBits(data, 4, 8, 0x39);
    assertEquals((byte) 0x03, data[0]);
    assertEquals((byte) 0x90, data[1]);
    MathUtil.setBits(data, 17, 7, 0x5f);
    assertEquals((byte) 0x5f, data[2]);
    MathUtil.setBits(data, 28, 2, 0x3);
    assertEquals((byte) 0x0c, data[3]);
    MathUtil.setBits(data, 0, 32, 0x63945F0C);
    assertEquals(0x63945F0C, MathUtil.getBits(data, 0, 32));
    MathUtil.setBits(data, 4, 8, 0);
    assertEquals(0x60045F0C, MathUtil.getBits(data, 0, 32));
  }

  public void testLog2() {
    assertEquals(-1, MathUtil.log2(0));
    assertEquals(0, MathUtil.log2(1));
    assertEquals(1, MathUtil.log2(2));
    assertEquals(1, MathUtil.log2(3));
    assertEquals(2, MathUtil.log2(4));
    assertEquals(6, MathUtil.log2(64));
    assertEquals(5, MathUtil.log2(63));
  }
}