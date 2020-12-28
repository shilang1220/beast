/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.geolite;

/**
 * Type of a non-geometric field in
 */
public enum FieldType {
  /**
   * Marker for string type (character array).
   */
  StringType((short) -1),
  /**
   * Marker for big-endian 32-bit integer type
   */
  IntegerType((short) 4),
  /**
   * Marker for big-endian 64-bit long type
   */
  LongType((short) 8),
  /**
   * Marker for big-endian 64-bit double-precision floating point type
   */
  DoubleType((short) 8),
  /**
   * A timestamp represents a date/time in milliseconds
   */
  TimestampType((short) 8),
  /**
   * A Boolean is either true or false
   */
  BooleanType((short) 1);

  /**
   * Size of this field in bytes or -1 if it is variable size
   */
  public short size;

  FieldType(short s) {
    this.size = s;
  }
}
