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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes LZW-compressed data from a TIFF file.
 * This code is based on the description in the TIFF file specs.
 * TODO improve the performance of this implementation
 */
public class LZWDecoder {

  /**A code to clear the message table*/
  public static final int ClearCode = 256;

  /**The code that signals the end of information*/
  public static final int EOI_CODE = 257;

  /** Marks part of the decoded message to be reused */
  static class PartialMessage {
    byte[] value;

    public PartialMessage(byte[] msg) {
      this.value = msg;
    }
  }

  /**
   * Decodes LZW-encoded message. See TIFF specs page 61 for details.
   * @param encodedData the encoded data as an array of bytes
   * @return the decoded data as an array of bytes
   */
  public static byte[] decode(byte[] encodedData) {
    ByteArrayOutputStream decodedMessage = new ByteArrayOutputStream();
    List<PartialMessage> decodeTable = initializeTable();
    int position = 0;
    int code, oldCode = -1;
    int length = 9;
    int totalNumBits = encodedData.length * 8;
    //while ((code = MathUtil.getBits(encodedData, position, Math.min(length, totalNumBits - position))) != EOI_CODE) {
    code = (int) MathUtil.getBits(encodedData, position, length);
    while (code != EOI_CODE && length < (totalNumBits-position) ) {
      position += length;
      if (code == ClearCode) {
        decodeTable = initializeTable();
        length = 9;
        code = (int) MathUtil.getBits(encodedData, position, length);
        position += length;
        if (code == EOI_CODE)
          break;
        writeString(decodedMessage, decodeTable.get(code).value);
        oldCode = code;
      } /* end of ClearCode case */
      else {

        if (code < decodeTable.size()) {
          // code in table
          writeString(decodedMessage, decodeTable.get(code).value);
          addStringToTable(decodeTable, concat(decodeTable.get(oldCode).value, decodeTable.get(code).value[0]));
          oldCode = code;
        } else {
          assert code == decodeTable.size();
          byte[] outString = concat(decodeTable.get(oldCode).value, decodeTable.get(oldCode).value[0]);
          writeString(decodedMessage, outString);
          addStringToTable(decodeTable, outString);
          oldCode = code;
        }
        if (decodeTable.size() - 1 == 510)
          length = 10;
        else if (decodeTable.size() - 1 == 1022)
          length = 11;
        else if (decodeTable.size() - 1 == 2046)
          length = 12;
      }
      code = (int) MathUtil.getBits(encodedData, position, Math.min(length, totalNumBits - position));
    }
    return decodedMessage.toByteArray();
  }

  private static void addStringToTable(List<PartialMessage> decodeTable, byte[] newMessage) {
    decodeTable.add(new PartialMessage(newMessage));
  }

  private static byte[] concat(byte[] value, byte b) {
    byte[] newMessage = new byte[value.length + 1];
    System.arraycopy(value, 0, newMessage, 0, value.length);
    newMessage[value.length] = b;
    return newMessage;
  }

  private static List<PartialMessage> initializeTable() {
    List<PartialMessage> table = new ArrayList();
    for (int $i = 0; $i < 256; $i++)
      table.add(new PartialMessage(new byte[] {(byte) $i}));
    table.add(null); // Clear Code
    table.add(null); // EOI
    return table;
  }

  /**
   * Appends data based on the given code
   * @param decodedMessage
   * @param value
   */
  private static void writeString(ByteArrayOutputStream decodedMessage, byte[] value) {
    try {
      decodedMessage.write(value);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
