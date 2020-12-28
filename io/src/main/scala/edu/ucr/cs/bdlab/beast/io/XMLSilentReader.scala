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
package edu.ucr.cs.bdlab.beast.io

import javax.xml.namespace.{NamespaceContext, QName}
import javax.xml.stream.{Location, XMLStreamConstants, XMLStreamException, XMLStreamReader}

/**
 * A delegate XML parser that absorbs any errors in the file. It can parse a subset of the file without
 * raising any parsing errors. It is used to split a big XML file into smaller parts and parse each split separately
 * without raising error when parsing the second or subsequent splits if starting at an arbitrary location.
 */
class XMLSilentReader(internalReader: XMLStreamReader) extends XMLStreamReader {

  /**
   * Try the given code block once and if an exception is thrown, it returns the default value.
   * @param fn a code block to execute
   * @param defaultValue a default value to be returned if the code throws an exception
   * @tparam T the type of the return value
   * @return either the value returned by the code block or the default value if the block throws an exception
   */
  private def absorbOne[T](fn: => T, defaultValue: T = null): T = {
    try {
      fn
    } catch {
      // Absorb the error
      case _: XMLStreamException => defaultValue
    }
  }

  /**
   * Keep retrying the given code block many times until it does not throw an error or until the end of the document
   * is reached [hasNext] return false. If the end of the document is reached, the default value is returned.
   * @param fn the code block to execute
   * @param defaultValue the default value to return if the end of the document is reached
   * @tparam T the type of the return value of the block and the type of the default value
   * @return either the return value of the code block or the default value if the end of the document is reached
   */
  private def absorbMany[T](fn: => T, defaultValue: T = null, retries: Int = 5): T = {
    var attempts: Int = retries
    while (internalReader.hasNext && attempts > 0) {
      try {
        return fn
      } catch {
        case e: XMLStreamException => /*e.printStackTrace();*/ attempts -= 1
      }
    }
    defaultValue
  }

  override def getProperty(s: String): AnyRef = internalReader.getProperty(s)

  override def next(): Int = absorbMany({internalReader.next}, XMLStreamConstants.END_DOCUMENT)

  override def require(i: Int, s: String, s1: String): Unit = absorbOne {internalReader.require(i, s, s1)}

  override def getElementText: String = absorbOne( {internalReader.getElementText}, null)

  override def nextTag(): Int = absorbMany({internalReader.nextTag()}, XMLStreamConstants.END_DOCUMENT)

  override def hasNext: Boolean = absorbMany({internalReader.hasNext()}, false)

  override def close(): Unit = absorbOne({internalReader.close()})

  override def getNamespaceURI(s: String): String = internalReader.getNamespaceURI(s)

  override def isStartElement: Boolean = internalReader.isStartElement

  override def isEndElement: Boolean = internalReader.isEndElement

  override def isCharacters: Boolean = internalReader.isCharacters

  override def isWhiteSpace: Boolean = internalReader.isWhiteSpace

  override def getAttributeValue(s: String, s1: String): String = internalReader.getAttributeValue(s, s1)

  override def getAttributeCount: Int = internalReader.getAttributeCount

  override def getAttributeName(i: Int): QName = internalReader.getAttributeName(i)

  override def getAttributeNamespace(i: Int): String = internalReader.getAttributeNamespace(i)

  override def getAttributeLocalName(i: Int): String = internalReader.getAttributeLocalName(i)

  override def getAttributePrefix(i: Int): String = internalReader.getAttributePrefix(i)

  override def getAttributeType(i: Int): String = internalReader.getAttributeType(i)

  override def getAttributeValue(i: Int): String = internalReader.getAttributeValue(i)

  override def isAttributeSpecified(i: Int): Boolean = internalReader.isAttributeSpecified(i)

  override def getNamespaceCount: Int = internalReader.getNamespaceCount

  override def getNamespacePrefix(i: Int): String = internalReader.getNamespacePrefix(i)

  override def getNamespaceURI(i: Int): String = internalReader.getNamespaceURI(i)

  override def getNamespaceContext: NamespaceContext = internalReader.getNamespaceContext

  override def getEventType: Int = internalReader.getEventType

  override def getText: String = internalReader.getText

  override def getTextCharacters: Array[Char] = internalReader.getTextCharacters

  override def getTextCharacters(i: Int, chars: Array[Char], i1: Int, i2: Int): Int = internalReader.getTextCharacters(i, chars, i1, i2)

  override def getTextStart: Int = internalReader.getTextStart

  override def getTextLength: Int = internalReader.getTextLength

  override def getEncoding: String = internalReader.getEncoding

  override def hasText: Boolean = internalReader.hasText

  override def getLocation: Location = internalReader.getLocation

  override def getName: QName = internalReader.getName

  override def getLocalName: String = internalReader.getLocalName

  override def hasName: Boolean = internalReader.hasName

  override def getNamespaceURI: String = internalReader.getNamespaceURI

  override def getPrefix: String = internalReader.getPrefix

  override def getVersion: String = internalReader.getVersion

  override def isStandalone: Boolean = internalReader.isStandalone

  override def standaloneSet(): Boolean = internalReader.standaloneSet()

  override def getCharacterEncodingScheme: String = internalReader.getCharacterEncodingScheme

  override def getPITarget: String = internalReader.getPITarget

  override def getPIData: String = internalReader.getPIData

}
